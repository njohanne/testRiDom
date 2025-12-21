package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // библиотека для работы с БД

	"github.com/njohanne/testRiDom/internal/config"
	"github.com/njohanne/testRiDom/internal/model"
)

type Repository struct {
	*sqlx.DB
}

func NewRepository(cfg *config.Postgres) (*Repository, error) {
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=disable",
		cfg.Username, cfg.Password, cfg.Database, cfg.Host, cfg.Port)

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	return &Repository{db}, nil
}

func (r *Repository) Close() {
	_ = r.DB.Close()
}

func (r *Repository) SaveEvent(ctx context.Context, msg *model.Event, taskKey string) error {
	sqlString, args, err := sq.Insert("event_logs").
		Columns("task_key", "event_name", "event_type", "event_number", "event_message", "processing_duration_ms", "created_at").
		Values(taskKey, msg.EventName, msg.EventType, msg.EventNumber, msg.EventMsg, msg.ProcessingDuration, msg.CreatedAt).
		PlaceholderFormat(sq.Dollar).ToSql()
	if err != nil {
		return fmt.Errorf("failed to build sql string: %w", err)
	}

	_, err = r.ExecContext(ctx, sqlString, args...)

	return err
}

///// Для воркера

func (r *Repository) UpdateEventForTaskKey(ctx context.Context, taskKey, status string) error {
	sqlString, args, err := sq.Update("event_logs").
		Set("status", status).
		Set("processed_at", sq.Expr("NOW()")).
		Where(sq.Eq{"task_key": taskKey}).
		PlaceholderFormat(sq.Dollar).ToSql()
	if err != nil {
		return fmt.Errorf("failed to build sql string: %w", err)
	}

	result, err := r.ExecContext(ctx, sqlString, args...)
	if err != nil {
		return fmt.Errorf("failed to execute update: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("no rows updated for task_key: %s", taskKey)
	}

	return nil
}

func (r *Repository) GetOldTask(ctx context.Context) (string, error) {
	tx, err := r.BeginTxx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err = tx.Rollback(); err != nil && err != sql.ErrTxDone {
			log.Printf("failed to rollback transaction: %v", err)
		}
	}()

	sqlString, args, err := sq.Select("task_key").
		From("event_logs").
		Where(sq.Eq{"status": "pending"}).
		OrderBy("created_at ASC").
		Limit(1).Suffix("FOR UPDATE SKIP LOCKED").
		PlaceholderFormat(sq.Dollar).ToSql()
	if err != nil {
		return "", err
	}

	var taskKey string

	err = r.GetContext(ctx, &taskKey, sqlString, args...)
	if err != nil {
		return "", err
	}

	err = r.UpdateEventForTaskKey(ctx, taskKey, "processing")
	if err != nil {
		return "", err
	}

	return taskKey, nil
}

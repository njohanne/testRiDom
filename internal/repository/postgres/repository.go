package postgres

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/njohanne/testRiDom/internal/config"
	"github.com/njohanne/testRiDom/internal/model"
)

type Repository struct {
	*sqlx.DB
}

func NewRepository(cfg config.Postgres) (*Repository, error) {
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

func (r *Repository) SaveEvent(msg model.Event) error {
	return nil
}

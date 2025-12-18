package postgres

import (
	"fmt"

	_ "github.com/lib/pq"

	"github.com/jmoiron/sqlx"
	"github.com/njohanne/testRiDom/internal/config"
)

type Repository struct {
	*sqlx.DB
}

func NewRepository(cfg config.Postgres) (*Repository, error) {
	if err := validateCfg(cfg); err != nil {
		return nil, err
	}

	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=disable",
		cfg.Username, cfg.Password, cfg.Database, cfg.Host, cfg.Port)

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	return &Repository{db}, nil
}

func validateCfg(cfg config.Postgres) error {
	if cfg.Username == "" {
		return fmt.Errorf("postgres username is empty")
	}
	if cfg.Password == "" {
		return fmt.Errorf("postgres password is empty")
	}
	if cfg.Host == "" {
		return fmt.Errorf("postgres host is empty")
	}
	if cfg.Port == "" {
		return fmt.Errorf("postgres port is empty")
	}
	if cfg.Database == "" {
		return fmt.Errorf("postgres database is empty")
	}
	return nil
}

func (r *Repository) Close() {
	_ = r.DB.Close()
}

package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/njohanne/testRiDom/internal/config"
	"github.com/redis/go-redis/v9"
)

type Repository struct {
	client *redis.Client
}

func NewClient(cfg config.Redis) (*Repository, error) {
	if err := validateCfg(cfg); err != nil {
		return nil, err
	}

	addrStr := fmt.Sprintf("%s:%s", cfg.Host, cfg.Port)
	client := redis.NewClient(&redis.Options{
		Addr:     addrStr,
		Password: cfg.Password,
		DB:       0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Ping(ctx).Result()
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return &Repository{client: client}, nil
}

func validateCfg(cfg config.Redis) error {
	if cfg.Host == "" {
		return fmt.Errorf("redis host is empty")
	}
	if cfg.Port == "" {
		return fmt.Errorf("redis port is empty")
	}
	return nil
}

func (r *Repository) Close() error {
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}

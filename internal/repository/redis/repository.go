package redis

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/njohanne/testRiDom/internal/config"
	"github.com/njohanne/testRiDom/internal/model"
	"github.com/redis/go-redis/v9"
)

type Repository struct {
	client *redis.Client
}

func NewClient(cfg config.Redis) (*Repository, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     net.JoinHostPort(cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Ping(ctx).Result()
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return &Repository{client: client}, nil
}

func (r *Repository) Close() {
	_ = r.client.Close()
}

func (r *Repository) SaveEvent(msg model.Event) error {
	return nil
}

package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/njohanne/testRiDom/internal/config"
	"github.com/njohanne/testRiDom/internal/model"
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

func (r *Repository) SaveEvent(ctx context.Context, msg *model.Event, taskKey string) error {
	eventJSON, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	err = r.client.Set(ctx, taskKey, eventJSON, 0).Err()
	if err != nil {
		return err
	}

	return nil
}

func (r *Repository) GetEvent(ctx context.Context, taskKey string) (*model.Event, error) {
	eventJSON, err := r.client.Get(ctx, taskKey).Bytes()
	if err != nil {
		return nil, err
	}

	var event model.Event

	err = json.Unmarshal(eventJSON, &event)
	if err != nil {
		return nil, err
	}

	return &event, nil
}

func (r *Repository) DeleteEvent(ctx context.Context, taskKey string) error {
	return r.client.Del(ctx, taskKey).Err()
}

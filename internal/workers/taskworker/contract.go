package taskworker

import (
	"context"

	"github.com/njohanne/testRiDom/internal/model"
)

type TaskWorker interface {
}

type RedisRepo interface {
	GetEvent(ctx context.Context, taskKey string) (*model.Event, error)
	DeleteEvent(ctx context.Context, taskKey string) error
}

type PostgresRepo interface {
	GetOldTask(ctx context.Context) (string, error)
	UpdateEventForTaskKey(ctx context.Context, taskKey, status string) error
}

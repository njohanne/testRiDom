package eventconsumer

import (
	"context"

	"github.com/segmentio/kafka-go"

	"github.com/njohanne/testRiDom/internal/model"
)

type EventConsumer interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	CommitMessage(ctx context.Context, msg *kafka.Message) error
}

type RedisRepo interface {
	SaveEvent(ctx context.Context, msg *model.Event, taskKey string) error // not reliase
}

type PostgresRepo interface {
	SaveEvent(ctx context.Context, msg *model.Event, taskKey string) error // not reliase
}

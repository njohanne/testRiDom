package eventconsumer

import (
	"context"

	"github.com/njohanne/testRiDom/internal/model"
	"github.com/segmentio/kafka-go"
)

type EventConsumer interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	CommitMessage(ctx context.Context, msg kafka.Message) error
}

type RedisRepo interface {
	SaveEvent(msg model.Event) error // not reliase
}

type PostgresRepo interface {
	SaveEvent(msg model.Event) error // not reliase
}

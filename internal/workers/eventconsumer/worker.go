package eventconsumer

import (
	"context"
	"fmt"
	"log"

	"github.com/njohanne/testRiDom/internal/model"
	"github.com/segmentio/kafka-go"
)

type Worker struct {
	ctx    context.Context
	cancel context.CancelFunc

	chEvents  chan kafka.Message
	eventCons EventConsumer
	redis     RedisRepo
	postgres  PostgresRepo
}

func New(eventCons EventConsumer, redisRepo RedisRepo, postgresRepo PostgresRepo) *Worker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Worker{
		ctx:       ctx,
		cancel:    cancel,
		chEvents:  make(chan kafka.Message),
		eventCons: eventCons,
		redis:     redisRepo,
		postgres:  postgresRepo,
	}
}

func (w *Worker) Start() {
	w.startConsumer()
	w.startWorker()
}

func (w *Worker) startConsumer() {
	go func() {
		for {
			select {
			case <-w.ctx.Done():
				return
			default:
				msg, err := w.eventCons.ReadMessage(w.ctx)
				if err != nil {
					log.Printf("failed to read message: %v", err)
				}
				w.chEvents <- msg
			}
		}
	}()
}

func (w *Worker) startWorker() {
	go func() {
		for {
			select {
			case <-w.ctx.Done():
				return
			default:
				for msg := range w.chEvents {
					event, err := model.KafkaMsgToEvent(msg)
					if err != nil {
						log.Printf("failed to convert message to event: %v", err)
					}

					err = w.postgres.SaveEvent(event)
					if err != nil {
						log.Printf("failed to save event: %v", err)
					}

					err = w.redis.SaveEvent(event)
					if err != nil {
						log.Printf("failed to save event: %v", err)
					}

					err = w.eventCons.CommitMessage(w.ctx, msg)
					if err != nil {
						log.Printf("failed to commit message: %v", err)
					}
					fmt.Println(msg)
				}
			}
		}
	}()
}

func (w *Worker) Stop() {
	w.cancel()
}

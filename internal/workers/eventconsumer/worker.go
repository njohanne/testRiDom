package eventconsumer

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/segmentio/kafka-go"

	"github.com/njohanne/testRiDom/internal/model"
)

type Worker struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

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
		wg:        sync.WaitGroup{},
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
	w.wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Consumer recovered from panic: %v", r)
			}
			w.wg.Done()
		}()
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
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case <-w.ctx.Done():
				return
			case msg, ok := <-w.chEvents:
				if !ok {
					return
				}

				event, err := model.KafkaMsgToEvent(&msg)
				taskKey := fmt.Sprintf("task:%d:%d", msg.Partition, msg.Offset)

				if err != nil {
					log.Printf("failed to convert message to event: %v", err)
					return
				}

				err = w.postgres.SaveEvent(w.ctx, &event, taskKey)
				if err != nil {
					log.Printf("failed to save event: %v", err)
					return
				}

				err = w.redis.SaveEvent(w.ctx, &event, taskKey)
				if err != nil {
					log.Printf("failed to save event: %v", err)
				}

				err = w.eventCons.CommitMessage(w.ctx, &msg)
				if err != nil {
					log.Printf("failed to commit message: %v", err)

				}
			}
		}
	}()
}

func (w *Worker) Stop() {
	w.cancel()
	w.wg.Wait()
	close(w.chEvents)
}

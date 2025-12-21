package taskworker

import (
	"context"
	"fmt"
	"time"
)

type Worker struct {
	ctx    context.Context
	cancel context.CancelFunc

	Redis    RedisRepo
	Postgres PostgresRepo
}

func New(redis RedisRepo, postgres PostgresRepo) *Worker {
	ctx, cancel := context.WithCancel(context.Background())

	return &Worker{ctx: ctx,
		cancel:   cancel,
		Redis:    redis,
		Postgres: postgres}
}

func (w *Worker) Start() {
	go func() {
		fmt.Println("starting taskWorker")

		for {
			select {
			case <-w.ctx.Done():
				return
			default:
				taskKey, err := w.Postgres.GetOldTask(w.ctx)
				if err != nil {
					fmt.Printf("GetOldTask err: %v\n", err)
					time.Sleep(5 * time.Second)

					continue
				}

				event, err := w.Redis.GetEvent(w.ctx, taskKey)
				if err != nil {
					fmt.Printf("GetEvent err: %v\n", err)
				}

				fmt.Println("Sleeping... ", event.ProcessingDuration)
				time.Sleep(event.ProcessingDuration)
				fmt.Println("good morning", event.ProcessingDuration)

				err = w.Postgres.UpdateEventForTaskKey(w.ctx, taskKey, "completed")
				if err != nil {
					fmt.Printf("UpdateEventForTaskKey err: %v\n", err)
				}

				err = w.Redis.DeleteEvent(w.ctx, taskKey)
				if err != nil {
					fmt.Printf("DeleteEvent err: %v\n", err)
				}

				fmt.Println("...good morning...")
			}
		}
	}()
}

func (w *Worker) Stop() {
	w.cancel()
}

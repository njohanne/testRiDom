package eventproducer

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/njohanne/testRiDom/internal/model"
)

type Worker struct {
	ctx    context.Context
	cancel context.CancelFunc

	eventPrd EventProducer
}

func New(producer EventProducer) *Worker {
	ctx, cancel := context.WithCancel(context.Background())

	return &Worker{
		ctx:      ctx,
		cancel:   cancel,
		eventPrd: producer,
	}
}

func (w *Worker) Start() {
	go func() {
		for {
			select {
			case <-w.ctx.Done():
				return
			default:
				event := w.generateEvent()

				err := w.eventPrd.SendMessage(&event)
				if err != nil {
					log.Printf("failed to send event: %v", err)
				}
			}
		}
	}()

}

func (w *Worker) Stop() {
	w.cancel()
}

func (w *Worker) generateEvent() model.Event {
	eventNames := []string{
		"UserLogin",
		"UserLogout",
		"OrderCreated",
		"OrderCancelled",
		"PaymentProcessed",
		"ItemAdded",
		"ItemRemoved",
		"SystemUpdate",
		"DataSync",
		"NotificationSent",
	}

	eventTypes := []string{
		"user_action",
		"system_event",
		"transaction",
		"notification",
		"data_change",
		"error",
		"info",
		"warning",
	}

	messages := []string{
		"User successfully logged in",
		"Order has been processed",
		"Payment transaction completed",
		"Data synchronization started",
		"System maintenance scheduled",
		"New notification received",
		"Configuration updated",
		"Error occurred during processing",
		"Task completed successfully",
		"Resource allocation changed",
	}

	event := model.Event{
		ProcessingTime: time.Now().Add(time.Duration(rand.Intn(86400)) * time.Second),
		EventName:      eventNames[rand.Intn(len(eventNames))],
		EventType:      eventTypes[rand.Intn(len(eventTypes))],
		EventNumber:    rand.Intn(1000000) + 1,
		EventMsg:       messages[rand.Intn(len(messages))],
	}

	return event
}

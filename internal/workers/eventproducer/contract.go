package eventproducer

import "github.com/njohanne/testRiDom/internal/model"

type EventProducer interface {
	SendMessage(message *model.Event) error
}

package model

import (
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

type Event struct {
	ProcessingTime time.Time
	EventName      string
	EventType      string
	EventNumber    int
	EventMsg       string
}

func KafkaMsgToEvent(msgKafka kafka.Message) (Event, error) {
	event := Event{}
	err := json.Unmarshal(msgKafka.Value, &event)
	return event, err
}

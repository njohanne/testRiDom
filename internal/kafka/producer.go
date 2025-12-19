package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/njohanne/testRiDom/internal/model"
	"github.com/segmentio/kafka-go"

	"github.com/njohanne/testRiDom/internal/config"
)

const defaultTimeout = time.Second * 10

type Producer struct {
	writer *kafka.Writer
	cfg    config.Kafka
}

func NewProducer(cfg config.Kafka) *Producer {
	return &Producer{cfg: cfg}
}

func (p *Producer) Connect() error {
	if p.writer != nil {
		return nil
	}

	brokerAddr := net.JoinHostPort(p.cfg.Host, p.cfg.Port)

	conn, err := kafka.Dial("tcp", brokerAddr)
	if err != nil {
		return fmt.Errorf("failed to kafka broker unreachable: %w", err)
	}

	defer conn.Close()

	p.writer = &kafka.Writer{Addr: kafka.TCP(brokerAddr),
		Topic:    p.cfg.Topic,
		Balancer: &kafka.LeastBytes{}}

	return nil
}

func (p *Producer) Close() {
	_ = p.writer.Close()
}

func (p *Producer) SendMessage(message *model.Event) error {
	if p.writer == nil {
		return fmt.Errorf("failed to kafka producer has not been initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	msgJSON, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	msg := kafka.Message{
		Value: msgJSON,
		Time:  time.Now(),
	}

	err = p.writer.WriteMessages(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to kafka send message: %w", err)
	}

	return nil
}

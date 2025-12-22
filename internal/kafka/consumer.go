package kafka

import (
	"context"
	"fmt"
	"net"

	"github.com/segmentio/kafka-go"

	"github.com/njohanne/testRiDom/internal/config"
)

type Consumer struct {
	reader *kafka.Reader
	cfg    config.Kafka
}

func NewConsumer(cfg config.Kafka) *Consumer {
	return &Consumer{cfg: cfg}
}

func (c *Consumer) Connect() error {
	if c.reader != nil {
		return nil
	}

	brokerAddr := net.JoinHostPort(c.cfg.Host, c.cfg.Port)

	conn, err := kafka.Dial("tcp", brokerAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to kafka: %v", err)
	}
	defer conn.Close()

	readerCfg := kafka.ReaderConfig{
		Brokers:        []string{brokerAddr},
		Topic:          c.cfg.Topic,
		GroupID:        c.cfg.Group,
		StartOffset:    kafka.LastOffset,
		CommitInterval: 0,
	}

	c.reader = kafka.NewReader(readerCfg)

	return nil
}

func (c *Consumer) Close() {
	_ = c.reader.Close()
}

func (c *Consumer) ReadMessage(ctx context.Context) (kafka.Message, error) {
	if c.reader == nil {
		return kafka.Message{}, fmt.Errorf("failed to kafka reader is nil")
	}

	return c.reader.ReadMessage(ctx)
}

func (c *Consumer) CommitMessage(ctx context.Context, msg *kafka.Message) error {
	if c.reader == nil {
		return fmt.Errorf("failed to kafka reader is nil")
	}

	return c.reader.CommitMessages(ctx, *msg)
}

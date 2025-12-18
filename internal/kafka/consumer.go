package kafka

import (
	"context"
	"fmt"

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

	if err := c.validateCfg(); err != nil {
		return err
	}

	brokerAddr := fmt.Sprintf("%s:%s", c.cfg.Host, c.cfg.Port)

	if err := c.pingBroker(brokerAddr); err != nil {
		return err
	}

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

func (c *Consumer) validateCfg() error {
	if c.cfg.Host == "" {
		return fmt.Errorf("failed to kafka host is empty")
	}

	if c.cfg.Port == "" {
		return fmt.Errorf("failed to kafka port is empty")
	}

	if c.cfg.Topic == "" {
		return fmt.Errorf("failed to kafka topic is empty")
	}
	return nil
}

func (c *Consumer) pingBroker(address string) error {
	conn, err := kafka.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to connect to kafka: %v", err)
	}
	defer conn.Close()
	return nil
}

func (c *Consumer) Close() error {
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}

func (c *Consumer) ReadMessage(ctx context.Context) (kafka.Message, error) {
	if c.reader == nil {
		return kafka.Message{}, fmt.Errorf("failed to kafka reader is nil")
	}
	return c.reader.ReadMessage(ctx)
}

func (c *Consumer) CommitMessage(ctx context.Context, msg kafka.Message) error {
	if c.reader == nil {
		return fmt.Errorf("failed to kafka reader is nil")
	}
	return c.reader.CommitMessages(ctx, msg)
}

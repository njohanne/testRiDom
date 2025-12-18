package kafka

import (
	"context"
	"fmt"
	"time"

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

	if err := p.validateCfg(); err != nil {
		return err
	}

	brokerAddr := fmt.Sprintf("%s:%s", p.cfg.Host, p.cfg.Port)

	if err := p.pingBroker(brokerAddr); err != nil {
		return err
	}

	p.writer = &kafka.Writer{Addr: kafka.TCP(brokerAddr),
		Topic:    p.cfg.Topic,
		Balancer: &kafka.LeastBytes{}}

	return nil
}

func (p *Producer) pingBroker(address string) error {
	conn, err := kafka.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to kafka broker unreachable: %w", err)
	}

	defer conn.Close()
	return nil
}

func (p *Producer) validateCfg() error {
	if p.cfg.Host == "" {
		return fmt.Errorf("failed to kafka host is empty")
	}
	if p.cfg.Port == "" {
		return fmt.Errorf("failed to kafka port is empty")
	}
	if p.cfg.Topic == "" {
		return fmt.Errorf("failed to kafka topic is empty")
	}
	return nil
}

func (p *Producer) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}

func (p *Producer) SendMessage(message string) error {
	if p.writer == nil {
		return fmt.Errorf("failed to kafka producer has not been initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	msg := kafka.Message{
		Value: []byte(message),
		Time:  time.Now(),
	}

	err := p.writer.WriteMessages(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to kafka send message: %w", err)
	}

	return nil
}

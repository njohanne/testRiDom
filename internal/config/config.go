package config

import (
	"fmt"
	"log"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Service  Service
	Postgres Postgres
	Kafka    Kafka
	Redis    Redis
}

type Service struct {
	Port string `env:"TEST_RIDOM_PORT"`
	Name string `env:"TEST_RIDOM_NAME"`
}

type Postgres struct {
	Host     string `env:"POSTGRES_HOST"`
	Port     string `env:"POSTGRES_PORT"`
	Username string `env:"POSTGRES_USERNAME"`
	Password string `env:"POSTGRES_PASSWORD"`
	Database string `env:"POSTGRES_DATABASE"`
}

type Kafka struct {
	Host  string `env:"KAFKA_HOST"`
	Port  string `env:"KAFKA_PORT"`
	Topic string `env:"KAFKA_TOPIC"`
	Group string `env:"KAFKA_GROUP_ID"`
}

type Redis struct {
	Host     string `env:"REDIS_HOST"`
	Port     string `env:"REDIS_PORT"`
	Password string `env:"REDIS_PASSWORD"`
}

func MustLoad() *Config {
	cfg := &Config{}
	if err := cleanenv.ReadEnv(cfg); err != nil {
		log.Fatalf("cannot read env variables: %s", err)
	}

	return cfg
}

func (c *Config) ValidateConfig() error {
	err := c.Kafka.validate()
	if err != nil {
		return err
	}

	err = c.Postgres.validate()
	if err != nil {
		return err
	}

	err = c.Redis.validateCfg()
	if err != nil {
		return err
	}

	return nil
}

func (k *Kafka) validate() error {
	if k.Host == "" {
		return fmt.Errorf("failed to kafka host is empty")
	}

	if k.Port == "" {
		return fmt.Errorf("failed to kafka port is empty")
	}

	if k.Topic == "" {
		return fmt.Errorf("failed to kafka topic is empty")
	}

	return nil
}

func (p *Postgres) validate() error {
	if p.Username == "" {
		return fmt.Errorf("postgres username is empty")
	}

	if p.Password == "" {
		return fmt.Errorf("postgres password is empty")
	}

	if p.Host == "" {
		return fmt.Errorf("postgres host is empty")
	}

	if p.Port == "" {
		return fmt.Errorf("postgres port is empty")
	}

	if p.Database == "" {
		return fmt.Errorf("postgres database is empty")
	}

	return nil
}

func (r *Redis) validateCfg() error {
	if r.Host == "" {
		return fmt.Errorf("redis host is empty")
	}

	if r.Port == "" {
		return fmt.Errorf("redis port is empty")
	}

	return nil
}

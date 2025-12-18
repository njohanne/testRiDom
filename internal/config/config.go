package config

import (
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

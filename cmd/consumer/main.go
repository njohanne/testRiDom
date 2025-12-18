package main

import (
	"log"

	"github.com/njohanne/testRiDom/internal/config"
	"github.com/njohanne/testRiDom/internal/kafka"
	"github.com/njohanne/testRiDom/internal/postgres"
	"github.com/njohanne/testRiDom/internal/redis"
)

func main() {
	cfg := config.MustLoad()

	cons := kafka.NewConsumer(cfg.Kafka)
	if err := cons.Connect(); err != nil {
		log.Fatal(err)
	}
	defer cons.Close()

	post, err := postgres.NewRepository(cfg.Postgres)
	if err != nil {
		log.Fatal(err)
	}
	defer post.Close()

	red, err := redis.NewClient(cfg.Redis)
	if err != nil {
		log.Fatal(err)
	}
	defer red.Close()
	
}

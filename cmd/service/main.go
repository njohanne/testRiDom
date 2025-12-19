package main

import (
	"fmt"
	"log"
	"time"

	"github.com/njohanne/testRiDom/internal/config"
	"github.com/njohanne/testRiDom/internal/kafka"
	"github.com/njohanne/testRiDom/internal/repository/postgres"
	"github.com/njohanne/testRiDom/internal/repository/redis"
	"github.com/njohanne/testRiDom/internal/workers/eventconsumer"
	"github.com/njohanne/testRiDom/internal/workers/eventproducer"
)

func main() {
	cfg := config.MustLoad()

	prod := kafka.NewProducer(cfg.Kafka)
	err := prod.Connect()
	if err != nil {
		log.Fatal(err)
	}
	defer prod.Close()

	cons := kafka.NewConsumer(cfg.Kafka)
	if err = cons.Connect(); err != nil {
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

	workerPrd := eventproducer.New(prod)

	workerPrd.Start()
	defer workerPrd.Stop()

	workerCons := eventconsumer.New(cons, red, post)
	workerCons.Start()
	defer workerCons.Stop()

	fmt.Println("Starting workers...")
	
	time.Sleep(200 * time.Second)
}

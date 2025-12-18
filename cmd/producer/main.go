package main

import (
	"log"

	"github.com/njohanne/testRiDom/internal/config"
	"github.com/njohanne/testRiDom/internal/kafka"
)

func main() {
	cfg := config.MustLoad()
	_ = cfg

	prod := kafka.NewProducer(cfg.Kafka)
	err := prod.Connect()
	if err != nil {
		log.Fatal(err)
	}
	defer prod.Close()

}

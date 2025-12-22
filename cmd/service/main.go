package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/njohanne/testRiDom/internal/config"
	"github.com/njohanne/testRiDom/internal/kafka"
	"github.com/njohanne/testRiDom/internal/repository/postgres"
	"github.com/njohanne/testRiDom/internal/repository/redis"
	"github.com/njohanne/testRiDom/internal/workers/eventconsumer"
	"github.com/njohanne/testRiDom/internal/workers/eventproducer"
	"github.com/njohanne/testRiDom/internal/workers/taskworker"
)

func main() {
	osSigChan := make(chan os.Signal, 1)
	shutdownChan := make(chan struct{}, 1)

	signal.Notify(osSigChan, syscall.SIGINT, syscall.SIGTERM)

	cfg := config.MustLoad()

	err := cfg.ValidateConfig()
	if err != nil {
		panic(err)
	}

	post, err := postgres.NewRepository(&cfg.Postgres)
	if err != nil {
		panic(err)
	}
	defer post.Close()

	red, err := redis.NewClient(cfg.Redis)
	if err != nil {
		panic(err)
	}
	defer red.Close()

	prod := kafka.NewProducer(cfg.Kafka)

	err = prod.Connect()
	if err != nil {
		panic(err)
	}
	defer prod.Close()

	consNun := 1
	consWorkers := make([]*eventconsumer.Worker, consNun)

	for i := 0; i < consNun; i++ {
		cons := kafka.NewConsumer(cfg.Kafka)
		if err = cons.Connect(); err != nil {
			panic(err)
		}
		defer cons.Close()

		consWorkers[i] = eventconsumer.New(cons, red, post)
	}

	//cons := kafka.NewConsumer(cfg.Kafka)
	//if err = cons.Connect(); err != nil {
	//	panic(err)
	//}
	//defer cons.Close()

	workerPrd := eventproducer.New(prod)

	workerPrd.Start()

	for _, cons := range consWorkers {
		cons.Start()
	}
	//workerCons := eventconsumer.New(cons, red, post)
	//workerCons.Start()

	fmt.Println("Start workers...")

	taskWorker := taskworker.New(red, post)
	taskWorker.Start()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()

		sig := <-osSigChan

		log.Printf("got OS shutdown signal: %s", sig)

		shutdownChan <- struct{}{}
	}()

	fmt.Println("Starting workers...")

	<-shutdownChan
	wg.Wait()
	workerPrd.Stop()

	for _, cons := range consWorkers {
		cons.Stop()
	}

	taskWorker.Stop()
	close(shutdownChan)
	signal.Stop(osSigChan)
	close(osSigChan)
}

package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	"os"
)

func main() {
	fmt.Println("Hello world!")
	godotenv.Load("../.env")
}

func newKafkaProducer() (producer *kafka.Producer, err error) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("BOOTSTRAP-SERVER"),
	}
	producer, err = kafka.NewProducer(configMap)
	return
}

package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	"log"
	"os"
)

type Consumer struct {
	kafkaConsumer *kafka.Consumer
}

func main() {
	// Loads .env file
	godotenv.Load("../.env")

	// Creates the kafka consumer instance
	consumer, err := newKafkaConsumer()
	if err != nil {
		log.Fatalln("kafka consumer creation error: ", err.Error())
	}

	consumer.consumeMessages()
}

func newKafkaConsumer() (consumer Consumer, err error) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("BOOTSTRAP-SERVER"),
		"client.id":         "goapp-consumer",
		"group.id":          "goapp-group",
		"auto.offset.reset": "earliest",
	}

	kafkaConsumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		return
	}

	topics := []string{os.Getenv("TOPIC")}
	kafkaConsumer.SubscribeTopics(topics, nil)

	consumer.kafkaConsumer = kafkaConsumer
	return
}

func (consumer Consumer) consumeMessages() {
	for {
		message, err := consumer.kafkaConsumer.ReadMessage(-1)
		if err == nil {
			fmt.Println(string(message.Value))
		}
	}
}

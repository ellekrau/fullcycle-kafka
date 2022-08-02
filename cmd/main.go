package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	"log"
	"os"
	"time"
)

func main() {
	// Load .env
	godotenv.Load("../.env")

	// Start kafka producer
	producer, err := newKafkaProducer()
	if err != nil {
		log.Fatalln(fmt.Errorf("kafka producer creation error: %s", err.Error()))
	}

	// Send kafka message
	err = produceKafkaMessage(nil, "Hello world!", os.Getenv("TOPIC"), producer)
	producer.Flush(10000)
	if err != nil {
		log.Fatalln(fmt.Errorf("kakfa produce message error: %s", err.Error()))
	}
}

func newKafkaProducer() (producer *kafka.Producer, err error) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("BOOTSTRAP-SERVER"),
	}
	producer, err = kafka.NewProducer(configMap)
	return
}

func produceKafkaMessage(key []byte, message string, topic string, producer *kafka.Producer) (err error) {
	kafkaMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value:     []byte(message),
		Key:       key,
		Timestamp: time.Now(),
	}
	err = producer.Produce(kafkaMessage, nil)
	return
}

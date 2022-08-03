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
	// Loads .env file
	godotenv.Load("../.env")

	// Starts the kafka producer
	producer, err := newKafkaProducer()
	if err != nil {
		log.Fatalln(fmt.Errorf("kafka producer creation error: %s", err.Error()))
	}

	// Sends kafka message using flush
	err = produceKafkaMessage(nil, "Message with flush!", os.Getenv("TOPIC"), producer, nil)
	producer.Flush(1000)
	if err != nil {
		log.Fatalln(fmt.Errorf("kakfa produce message error: %s", err.Error()))
	}

	// Sends a kafka message using delivery channel
	deliveryChannel := make(chan kafka.Event)
	err = produceKafkaMessage(nil, "Message with delivery channel!", os.Getenv("TOPIC"), producer, deliveryChannel)

	// Receives the kafka message response sent to deliveryChannel
	//sentMessage := (<-deliveryChannel).(*kafka.Message)
	//if err = sentMessage.TopicPartition.Error; err != nil {
	//	log.Fatalln(fmt.Errorf("send kafka message error: %s", err.Error()))
	//}

	go runKafkaMessageDeliveryReport(deliveryChannel)
	producer.Flush(1000)
}

func newKafkaProducer() (producer *kafka.Producer, err error) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("BOOTSTRAP-SERVER"),
	}
	producer, err = kafka.NewProducer(configMap)
	return
}

func produceKafkaMessage(key []byte, message string, topic string, producer *kafka.Producer, deliveryChannel chan kafka.Event) (err error) {
	kafkaMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value:     []byte(message),
		Key:       key,
		Timestamp: time.Now(),
	}
	err = producer.Produce(kafkaMessage, deliveryChannel)
	return
}

func runKafkaMessageDeliveryReport(deliveryChan chan kafka.Event) {
	for event := range deliveryChan {
		switch event.(type) {
		case *kafka.Message:
			if err := event.(*kafka.Message).TopicPartition.Error; err != nil {
				log.Fatalln(fmt.Errorf("send kafka message error: %s", err.Error()))
			}
			log.Printf("message was successfully sent to '%s'", event.(*kafka.Message).TopicPartition)
		}
	}
}

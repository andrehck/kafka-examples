package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Order struct {
	id    int     `json:"id"`
	name  string  `json:"name"`
	valor float64 `json:"valor"`
	email string  `json:"email"`
}

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}

	var newOrder = Order{id: 123, name: "André Luis Ferreira", valor: 100.60, email: "andreferreiratrc@gmail.com"}
	data, _ := json.Marshal(newOrder)

	if err != nil {
		log.Fatalf("Json marshaling failed: %s", err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivery to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := []string{"ECOMMERCE_NEW_ORDER", "ECOMMERCE_SEND_EMAIL"}

	time.Sleep(2)
	for topicSend := range topic {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic[topicSend], Partition: kafka.PartitionAny},
			Value:          []byte(data),
			Key:            []byte(string(newOrder.id)),
		}, nil)
	}

	p.Flush(15 * 1000)
}

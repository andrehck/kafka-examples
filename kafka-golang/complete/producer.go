package main

import (
	"complete/emailkafka"
	"complete/fraude"
	"complete/logskafka"
	"complete/typeskafka"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	var wg sync.WaitGroup
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}

	var newOrder = typeskafka.Order{Id: 123, Name: "André Luis Ferreira", Valor: 100.60, Email: "andreferreiratrc@gmail.com"}
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

	for topicSend := range topic {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic[topicSend], Partition: kafka.PartitionAny},
			Value:          []byte(data),
			Key:            []byte(string(rune(newOrder.Id))),
		}, nil)
	}

	p.Flush(15 * 1000)

	wg.Add(3)
	go fraude.Fraude(wg)
	go emailkafka.SendTopicEmail(wg)
	go logskafka.Logskafka(wg)
	wg.Wait()

}

package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9092",
		"group.id":          "FraudeConsumer",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"ECOMMERCE_NEW_ORDER"}, nil)

	run := true
	go twoConsumer()
	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			fmt.Println("Processamento de Fraude 1")
			fmt.Printf("Message on %s: %s %s offset:%s %s\n", msg.TopicPartition, string(msg.Value), msg.Timestamp, msg.TopicPartition.Offset, msg.Key)
		} else if !err.(kafka.Error).IsTimeout() {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}

func twoConsumer() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9092",
		"group.id":          "FraudeConsumer2",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"ECOMMERCE_NEW_ORDER"}, nil)

	run := true
	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			fmt.Println("Processamento de Fraude 2")
			fmt.Printf("Message on %s: %s %s offset:%s %s\n", msg.TopicPartition, string(msg.Value), msg.Timestamp, msg.TopicPartition.Offset, msg.Key)
		} else if !err.(kafka.Error).IsTimeout() {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}

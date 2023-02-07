package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9092",
		"group.id":          "EmailConsumer",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"ECOMMERCE_SEND_EMAIL"}, nil)

	run := true

	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			fmt.Println("Processamento de Email")
			fmt.Printf("Message %s\n on topic %s with the key: %s", string(msg.Value), msg.TopicPartition, msg.Key)
		} else if !err.(kafka.Error).IsTimeout() {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}

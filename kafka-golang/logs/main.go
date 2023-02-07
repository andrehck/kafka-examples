package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Order struct {
	id    int     `json:"id"`
	name  string  `json:"name"`
	valor float64 `json:"valor"`
	email string  `json:"email"`
}

type Color struct {
	Space string
	Point json.RawMessage // delay parsing until we know the color space
}

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9092",
		"group.id":          "LogConsumer",
	})

	if err != nil {
		panic(err)
	}
	//pattern := regexp.MustCompile("ECOMMERCE[A-Z]")
	c.SubscribeTopics([]string{"ECOMMERCE_NEW_ORDER", "ECOMMERCE_SEND_EMAIL"}, nil)

	run := true
	var newOrder []Color
	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			json.Unmarshal(msg.Value, &newOrder)

			fmt.Println(newOrder[0])

			fmt.Println("Processamento de logs")
			//fmt.Printf("Message on %s: %s %s offset:%s %s\n", msg.TopicPartition, newOrder.email, msg.Timestamp, msg.TopicPartition.Offset, msg.Key)
		} else if !err.(kafka.Error).IsTimeout() {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}

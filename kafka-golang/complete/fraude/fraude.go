package fraude

import (
	"complete/typeskafka"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func Fraude(wg1 sync.WaitGroup) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9092",
		"group.id":          "FraudeConsumer",
	})

	if err != nil {
		panic(err)
	}
	//pattern := regexp.MustCompile("ECOMMERCE[A-Z]")
	c.SubscribeTopics([]string{"ECOMMERCE_NEW_ORDER", "ECOMMERCE_SEND_EMAIL"}, nil)

	run := true
	var newOrder typeskafka.Order
	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {

			fmt.Println("Processamento de fraude")
			json.Unmarshal(msg.Value, &newOrder)
			fmt.Println(newOrder)

		} else if !err.(kafka.Error).IsTimeout() {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			defer wg1.Done()
		}
	}

	c.Close()
}

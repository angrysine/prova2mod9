package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var ProducerPointer *kafka.Producer 

func Producer() *kafka.Producer {
	if ProducerPointer == nil {
		ProducerPointer= GenerateProducer()
	}
	return ProducerPointer
}

func GenerateProducer() *kafka.Producer {
	conf := ReadConfig()
	p, _ := kafka.NewProducer(&conf)
	return p
}

func ProduceMessage(producer *kafka.Producer, topic string, amount int,message []byte) {
	
	for i := 0; i < amount; i++ {
	// produces a sample message to the user-created topic
	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(time.Now().Format("January 02, 2006 15:04:05")),
		Value:          message,
	}, nil)

	// send any outstanding or buffered messages to the Kafka broker and close the connection
	producer.Flush(15 * 1000)
	time.Sleep( 50*time.Millisecond)
	}
}

func GenerateMessage() []byte{
	id := "sensor_"+ fmt.Sprintf("%v",rand.Intn(30))
	timestamp := time.Now().Format("January 02, 2006 15:04:05")
	seed := rand.Float64()*100
	var tipoPoluente string
	if seed <=50 {
		tipoPoluente = "PM2.5"
	} else {
		tipoPoluente = "PM10"
	}
	datajson, err := json.Marshal(map[string]interface{}{"idSensor": id, "timestamp": timestamp, "tipoPoluente": tipoPoluente, "nivel": seed})
	if err != nil {
		panic(err)
	}
	return datajson
}

func ReadConfig() kafka.ConfigMap {
    // reads the client configuration from client.properties
    // and returns it as a key-value map
    m := make(map[string]kafka.ConfigValue)

    file, err := os.Open("client.properties")
    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed to open file: %s", err)
        os.Exit(1)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        line := strings.TrimSpace(scanner.Text())
        if !strings.HasPrefix(line, "#") && len(line) != 0 {
            kv := strings.Split(line, "=")
            parameter := strings.TrimSpace(kv[0])
            value := strings.TrimSpace(kv[1])
            m[parameter] = value
        }
    }

    if err := scanner.Err(); err != nil {
        fmt.Printf("Failed to read file: %s", err)
        os.Exit(1)
    }

    return m
}

// func main() {
// 	producer := Producer()
	
// 	ProduceMessage(producer,"qualidadeAr",30,GenerateMessage())
// }
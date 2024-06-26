package main

import (
	"fmt"
	"testing"
	 
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.mongodb.org/mongo-driver/mongo"
)

var ConsumerPointer *kafka.Consumer
var CollectionPointer *mongo.Collection

func Consumer() *kafka.Consumer {

	if ConsumerPointer == nil {
		GenerateConsumer()
		return ConsumerPointer
	}
	return ConsumerPointer
}

func GenerateConsumer() {
	// Configurações do consumidor
	configmap := ReadConfig()
	consumer, err := kafka.NewConsumer(&configmap)
	if err != nil {
		panic(err)
	}
	// defer consumer.Close()

	ConsumerPointer = consumer
}

func SubscribeIntegrity(consumer *kafka.Consumer,topic string, t *testing.T,message []byte) {
	// Assinar tópico
	err := consumer.SubscribeTopics([]string{topic}, nil)

	if err != nil {
		t.Errorf("Error subscribing: %v", err)
	} else {
		t.Log("Subscripton in topic north succeeded")
	}

	// Consumir mensagens
	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Received message: %s\n", string(msg.Value))
			if string(msg.Value) == string(message) {
				t.Log("Message integrity test passed")
				break

				} else {
					t.Errorf("Message integrity test failed")
					break
				}
		} else {
			t.Logf("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}
}

func SubscribeSemTeste(consumer *kafka.Consumer,topic string) {
	// Assinar tópico
	err := consumer.SubscribeTopics([]string{topic}, nil)

	if err != nil {
		panic(err)
	}
	// Consumir mensagens

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Received message: %s\n", string(msg.Value))
		}
	}
}



// func ReadConfig() kafka.ConfigMap {
//     // reads the client configuration from client.properties
//     // and returns it as a key-value map
//     m := make(map[string]kafka.ConfigValue)

//     file, err := os.Open("client.properties")
//     if err != nil {
//         fmt.Fprintf(os.Stderr, "Failed to open file: %s", err)
//         os.Exit(1)
//     }
//     defer file.Close()

//     scanner := bufio.NewScanner(file)
//     for scanner.Scan() {
//         line := strings.TrimSpace(scanner.Text())
//         if !strings.HasPrefix(line, "#") && len(line) != 0 {
//             kv := strings.Split(line, "=")
//             parameter := strings.TrimSpace(kv[0])
//             value := strings.TrimSpace(kv[1])
//             m[parameter] = value
//         }
//     }

//     if err := scanner.Err(); err != nil {
//         fmt.Printf("Failed to read file: %s", err)
//         os.Exit(1)
//     }

//     return m
// }


// func main() {
// 	consumer := Consumer()
// 	SubscribeSemTeste(consumer,"qualidadeAr")
// }
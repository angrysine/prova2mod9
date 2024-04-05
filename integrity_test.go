package main

import (
	"testing"
)



func TestIntegrity(t *testing.T) {
	consumer := Consumer()
	producer := Producer()
	message := GenerateMessage()
	go SubscribeIntegrity(consumer,"qualidadeAr",t,message)
	go ProduceMessage(producer,"qualidadeAr",1,message)
	
}
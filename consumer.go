package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Shopify/sarama"
)

func main() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGKILL)
	cg, _ := inClusterKafkaConfig()

	consumerGroup, err := sarama.NewConsumerGroup([]string{"my-cluster-kafka-bootstrap:9092"}, "groupid19231", cg)
	if err != nil {
		log.Printf("Error creating the Sarama consumer: %v", err)
		os.Exit(1)
	}

	cgh := &consumerGroupHandler{
		ready: make(chan bool),
		end:   make(chan int, 1),
		done:  make(chan bool),
	}
	ctx := context.Background()
	go func() {
		for {
			// this method calls the methods handler on each stage: setup, consume and cleanup
			consumerGroup.Consume(ctx, []string{"my-topic"}, cgh)
		}

	}()

	<-cgh.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	// waiting for the end of all messages received or an OS signal
	select {
	case <-cgh.end:
		log.Printf("Finished to receive %d messages\n", 100)
	case sig := <-signals:
		log.Printf("Got signal: %v\n", sig)
		close(cgh.done)
	}

	err = consumerGroup.Close()
	if err != nil {
		log.Printf("Error closing the Sarama consumer: %v", err)
		os.Exit(1)
	}
	log.Printf("Consumer closed")
}

// struct defining the handler for the consuming Sarama method
type consumerGroupHandler struct {
	ready chan bool
	end   chan int
	done  chan bool
}

func (cgh *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	close(cgh.ready)
	log.Printf("Consumer group handler setup\n")
	return nil
}

func (cgh *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Printf("Consumer group handler cleanup\n")
	return nil
}

func retrieveKey() {
	client := &http.Client{}
	var data = strings.NewReader("{\"maa_endpoint\": \"" + os.Getenv("SkrClientMAAEndpoint") + "\", \"akv_endpoint\": \"" + os.Getenv("SkrClientAKVEndpoint") + "\", \"kid\": \"" + os.Getenv("SkrClientKID") + "\"}")
	req, err := http.NewRequest("POST", "http://localhost:8080/key/release", data)
	if err != nil {

		log.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	bodyText, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("printing out the bodytte")
	fmt.Printf("%s\n", bodyText)
}

func (cgh *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	retrieveKey()
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}
			log.Printf("Message received: value=%s, partition=%d, offset=%d", string(message.Value), message.Partition, message.Offset)
			session.MarkMessage(message, "")
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-cgh.done:
			return nil
		}
	}
}

func inClusterKafkaConfig() (kafkaConfig *sarama.Config, err error) {
	// version, err := sarama.ParseKafkaVersion("2.11")
	// if err != nil {
	// 	log.Panicf("Error parsing Kafka version: %v", err)
	// }
	kafkaConfig = sarama.NewConfig()

	//kafkaConfig.ClientID = "kafka-on-kata-cc-mariner"

	//kafkaConfig.Producer.Partitioner = sarama.NewManualPartitioner
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	kafkaConfig.Version = sarama.V0_10_2_1

	return kafkaConfig, nil
}

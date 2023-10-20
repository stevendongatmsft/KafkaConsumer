package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Shopify/sarama"
)

type OuterJSON struct {
	Key string `json:"key"`
}

type JWKData struct {
	N string `json:"n"`
	E string `json:"e"`
	D string `json:"d"`
	P string `json:"p"`
	Q string `json:"q"`
}

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
	fmt.Printf("%s\n", string(bodyText))
	rsaPrivatekey, err := RSAPrivateKeyFromJWK(bodyText)
	if err != nil {
		fmt.Println("cannot convertrsa key")
	}
	fmt.Printf("keyis : %+v", rsaPrivatekey)
	var plaintext []byte
	ad := "ZPk4ZUx9uQOmsb4JxEdG8rQXXuSCZNWdDdNxFJKUQSQRPatbHjL6hBI2hj5k2scqEC+AlwD5a6KeLuYxOHBfz2A3aPI0jIyC4DSFAdOVTJo/jAPQbsOU/j9kceM+Di7n0dBqFKMf5McvANDYkNqmR8ICRWPrhbsjvVXtviwV2M7x7FZUn0RveIIs3vgi2zgWxh7ceVUs3Ei05zH6E70lAmKf/pWZL9ArkHcNJ/NmQ1DtmWcjmVWNdTmh/Mf5GRcTjO1UJljhTQerWvtfsMGg3jP+p4f561jBbJqLIuPveO8dM76A0qmrLPppJG5+nXeTF/It4MgW5ZZP0XCDBSmQhh3VvKjn2ywlOaTw0b9ix518RraO5WIbFxLT/iVmLDx6UlUiXtit7C9lEMXMUhL8Jhph0mLJ+lPS6iiLM0rXZjjlVE7HdDWH/J//d7707LG5SViNqnB1x56NHJJx3TPlbVe168EA4/0GmFSPtzRtb7pRwxPzwwJ+OtbacznBusCI"
	plaintext, err = rsa.DecryptOAEP(sha256.New(), rand.Reader, rsaPrivatekey, []byte(ad), nil)
	if err != nil {
		fmt.Println("unwrapp failed")
	}
	fmt.Println("plain data ", plaintext)
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

func RSAPrivateKeyFromJWK(jwkJSONBytes []byte) (*rsa.PrivateKey, error) {
	// Unmarshal the outer JSON
	var outer OuterJSON
	if err := json.Unmarshal(jwkJSONBytes, &outer); err != nil {
		return nil, fmt.Errorf("Error unmarshalling outer JSON: %v", err)
	}

	// Now, unmarshal the inner JWK JSON
	var jwk JWKData
	if err := json.Unmarshal([]byte(outer.Key), &jwk); err != nil {
		return nil, fmt.Errorf("Error unmarshalling inner JWK JSON: %v", err)
	}

	// Decode base64 values and build the RSA key
	n, err := base64.RawURLEncoding.DecodeString(jwk.N)
	if err != nil {
		return nil, err
	}
	e, err := base64.RawURLEncoding.DecodeString(jwk.E)
	if err != nil {
		return nil, err
	}
	d, err := base64.RawURLEncoding.DecodeString(jwk.D)
	if err != nil {
		return nil, err
	}
	p, err := base64.RawURLEncoding.DecodeString(jwk.P)
	if err != nil {
		return nil, err
	}
	q, err := base64.RawURLEncoding.DecodeString(jwk.Q)
	if err != nil {
		return nil, err
	}

	key := &rsa.PrivateKey{
		PublicKey: rsa.PublicKey{
			N: new(big.Int).SetBytes(n),
			E: int(new(big.Int).SetBytes(e).Int64()),
		},
		D: new(big.Int).SetBytes(d),
		Primes: []*big.Int{
			new(big.Int).SetBytes(p),
			new(big.Int).SetBytes(q),
		},
	}

	return key, nil
}

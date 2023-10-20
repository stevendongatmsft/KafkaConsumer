package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"

	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"

	"github.com/Microsoft/confidential-sidecar-containers/pkg/attest"
	"github.com/Microsoft/confidential-sidecar-containers/pkg/skr"
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

func (cgh *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}

			// Decrypt the message here
			plaintext, err := decryptMessage(message.Value)
			if err != nil {
				log.Printf("Error decrypting message: %v", err)
				continue
			}

			log.Printf("Message received: value=%s, partition=%d, offset=%d", string(plaintext), message.Partition, message.Offset)
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

func decryptMessage(ciphertext []byte) ([]byte, error) {
	// Logic for decryption using the unwrapping logic from UnWrapKey function
	// Return the decrypted message
	// Convert ciphertext to the expected input format
	var input keyProviderInput
	str := string(ciphertext)
	err := json.Unmarshal(ciphertext, &input)
	if err != nil {
		return nil, fmt.Errorf("Ill-formed input: %v. Error: %v", str, err)
	}

	// Extract decryption parameters
	var dc = input.KeyUnwrapParams.Dc
	if len(dc.Parameters["attestation-agent"]) == 0 {
		return nil, fmt.Errorf("attestation-agent must be specified in decryption config parameters: %v", str)
	}
	aa, _ := base64.StdEncoding.DecodeString(dc.Parameters["attestation-agent"][0])

	if string(aa) != "aasp" && string(aa) != "aaa" {
		return nil, fmt.Errorf("Unexpected attestation agent %v specified", string(aa))
	}

	annotationBytes, err := base64.StdEncoding.DecodeString(input.KeyUnwrapParams.Annotation)
	if err != nil {
		return nil, fmt.Errorf("Annotation is not a base64 encoding: %v. Error: %v", input.KeyUnwrapParams.Annotation, err)
	}

	var annotation AnnotationPacket
	err = json.Unmarshal(annotationBytes, &annotation)
	if err != nil {
		return nil, fmt.Errorf("Ill-formed annotation packet: %v. Error: %v", input.KeyUnwrapParams.Annotation, err)
	}

	bearerToken := ""
	clientID := os.Getenv("AZURE_CLIENT_ID")
	tenantID := os.Getenv("AZURE_TENANT_ID")
	tokenFile := os.Getenv("AZURE_FEDERATED_TOKEN_FILE")
	if clientID != "" && tenantID != "" && tokenFile != "" {
		bearerToken, err = getAccessTokenFromFederatedToken(context.Background(), tokenFile, clientID, tenantID, "https://managedhsm.azure.net")
		if err != nil {
			return nil, fmt.Errorf("Failed to obtain access token to MHSM: %v", err)
		}
	}

	mhsm := skr.MHSM{
		Endpoint:    annotation.KmsEndpoint,
		APIVersion:  "api-version=7.3-preview",
		BearerToken: bearerToken,
	}

	maa := attest.MAA{
		Endpoint:   annotation.AttesterEndpoint,
		TEEType:    "SevSnpVM",
		APIVersion: "api-version=2020-10-01",
	}

	skrKeyBlob := skr.KeyBlob{
		KID:       annotation.Kid,
		Authority: maa,
		MHSM:      mhsm,
	}

	keyBytes, err := skr.SecureKeyRelease("", azure_info.CertCache, azure_info.Identity, skrKeyBlob)
	if err != nil {
		return nil, fmt.Errorf("SKR failed: %v", err)
	}

	key, err := x509.ParsePKCS8PrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("Released key is invalid: %v", err)
	}

	var plaintext []byte
	if privkey, ok := key.(*rsa.PrivateKey); ok {
		plaintext, err = rsa.DecryptOAEP(sha256.New(), rand.Reader, privkey, annotation.WrappedData, nil)
		if err != nil {
			return nil, fmt.Errorf("Unwrapping failed: %v", err)
		}
	} else {
		return nil, fmt.Errorf("Released key is not a RSA private key: %v", err)
	}

	return plaintext, nil
}

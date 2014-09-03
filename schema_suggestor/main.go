package main

import (
	"bufio"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/twitchscience/aws_utils/listener"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/sqs"
)

type BPHandler struct{}

func (handler *BPHandler) Handle(message *sqs.Message) error {
	err := json.Unmarshal([]byte(msg.Body), &edgeMessage)
	if err != nil {
		return fmt.Errorf("Could not decode %s\n", msg.Body)
	}
	parts := strings.SplitN(edgeMessage.Keyname, "/", 2)
	bucket := s.S3.Bucket(parts[0])

	readCloser, err := bucket.GetReader(parts[1])
	if err != nil {
		return fmt.Errorf("Unable to get s3 reader %s on bucket %s with key %s\n",
			err, "spade-edge-"+env, edgeMessage.Keyname)
	}
	defer readCloser.Close()

	b := make([]byte, 8)
	rand.Read(b)

	tmpFile, err := ioutil.TempFile("", fmt.Sprintf("%08x", b))
	log.Println("using ", edgeMessage.Keyname)
	if err != nil {
		return fmt.Errorf("cloud not open temp file for %s as %s:  %s\n",
			edgeMessage.Keyname, fmt.Sprintf("%08x", b), err)
	}
	defer os.Remove(tmpFile.Name())
	writer := bufio.NewWriter(tmpFile)
	n, err := writer.ReadFrom(readCloser)
	if err != nil {
		return fmt.Errorf("Encounted err while downloading %s: %s\n", edgeMessage.Keyname, err)
	}
	writer.Flush()
	log.Printf("Downloaded a %d byte file\n", n)

	tmpStat, err := tmpFile.Stat()
	if err != nil {
		return fmt.Errorf("Unable to stat %s\n", tmpFile.Name())
	}
}

func main() {
	// SQS listener pools SQS queue and then kicks off a jobs to
	// suggest the schemas.
	auth, err := aws.GetAuth("", "", "", time.Now())
	if err != nil {
		log.Fatalf("Failed to recieve auth from env: %s\n", err)
	}

	poller := listener.BuildSQSListener(
		&listener.SQSAddr{
			Region:    aws.USWest2,
			QueueName: "test",
			Auth:      auth,
		},
		&BPHandler{},
		2*time.Minutes,
	)
	go poller.Listen()

	sigc := make(chan os.Signal, 1)
	wait := make(chan bool)
	signal.Notify(sigc,
		syscall.SIGINT)
	go func() {
		<-sigc
		// Cause flush
		poller.Close()
		// TODO: rethink the auditlogger logic...
		wait <- true
	}()

	<-wait

	// need:
	// cache of current tables (twitchscience/blueprint/scoopclient/cachingclient/client.go)
	// processor (processor.go)
	// outputter (outputter.go)
}

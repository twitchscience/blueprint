package main

import (
	"bufio"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twitchscience/aws_utils/environment"
	"github.com/twitchscience/aws_utils/listener"
	"github.com/twitchscience/blueprint/schema_suggestor/processor"
	cachingscoopclient "github.com/twitchscience/blueprint/scoopclient/cachingclient"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/crowdmob/goamz/sqs"
)

var (
	scoopProto      = flag.String("proto", "http", "the protocol to use when connecting to scoop")
	scoopHostname   = flag.String("hostname", "localhost", "the host to connect to scoop on")
	scoopPort       = flag.Uint64("port", 8080, "the port to connect to scoop on")
	staticFileDir   = flag.String("staticfiles", "./static/events", "the location to serve static files from")
	transformConfig = flag.String("transformConfig", "transforms_available.json", "config for available transforms in spade")
	env             = environment.GetCloudEnv()
)

func scoopUrl() string {
	return fmt.Sprintf("%s://%s:%d", *scoopProto, *scoopHostname, *scoopPort)
}

type BPHandler struct {
	Router *processor.EventRouter
	S3     *s3.S3
}

type NontrackedLogMessage struct {
	Tablename string
	Keyname   string
}

func (handler *BPHandler) Handle(msg *sqs.Message) error {
	var rotatedMessage NontrackedLogMessage

	err := json.Unmarshal([]byte(msg.Body), &rotatedMessage)
	if err != nil {
		return fmt.Errorf("Could not decode %s\n", msg.Body)
	}
	parts := strings.SplitN(rotatedMessage.Keyname, "/", 2)
	bucket := handler.S3.Bucket(parts[0])

	readCloser, err := bucket.GetReader(parts[1])
	if err != nil {
		return fmt.Errorf("Unable to get s3 reader %s on bucket %s with key %s\n",
			err, "spade-nontracked-"+env, rotatedMessage.Keyname)
	}
	defer readCloser.Close()

	b := make([]byte, 8)
	rand.Read(b)

	tmpFile, err := ioutil.TempFile("", fmt.Sprintf("%08x", b))

	if err != nil {
		return fmt.Errorf("cloud not open temp file for %s as %s:  %s\n",
			rotatedMessage.Keyname, fmt.Sprintf("%08x", b), err)
	}
	defer os.Remove(tmpFile.Name())
	writer := bufio.NewWriter(tmpFile)
	_, err = writer.ReadFrom(readCloser)
	if err != nil {
		return fmt.Errorf("Encounted err while downloading %s: %s\n", rotatedMessage.Keyname, err)
	}
	writer.Flush()

	tmpStat, err := tmpFile.Stat()
	if err != nil {
		return fmt.Errorf("Unable to stat %s\n", tmpFile.Name())
	}
	log.Println("using " + os.TempDir() + "/" + tmpStat.Name())

	return handler.Router.ReadFile(os.TempDir() + "/" + tmpStat.Name())
}

func main() {
	flag.Parse()
	scoopClient := cachingscoopclient.New(scoopUrl(), *transformConfig)

	// SQS listener pools SQS queue and then kicks off a jobs to
	// suggest the schemas.
	auth, err := aws.GetAuth("", "", "", time.Now())
	if err != nil {
		log.Fatalf("Failed to recieve auth from env: %s\n", err)
	}
	s3Connection := s3.New(
		auth,
		aws.USWest2,
	)

	poller := listener.BuildSQSListener(
		&listener.SQSAddr{
			Region:    aws.USWest2,
			QueueName: "spade-nontracked-" + env,
			Auth:      auth,
		},
		&BPHandler{
			Router: processor.NewRouter(
				*staticFileDir,
				5*time.Minute,
				scoopClient,
			),
			S3: s3Connection,
		},
		2*time.Minute,
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

		wait <- true
	}()

	<-wait
}

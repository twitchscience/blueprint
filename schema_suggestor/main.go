package main

import (
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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/twitchscience/aws_utils/listener"
	"github.com/twitchscience/blueprint/schema_suggestor/processor"
	cachingscoopclient "github.com/twitchscience/blueprint/scoopclient/cachingclient"
)

var (
	scoopURL        = flag.String("url", "http://localhost:8080", "the url to talk to scoop")
	staticFileDir   = flag.String("staticfiles", "./static/events", "the location to serve static files from")
	transformConfig = flag.String("transformConfig", "transforms_available.json", "config for available transforms in spade")
	nonTrackedQueue = flag.String("nonTrackedQueue", "", "SQS Queue name to listen to for nontracked events.")
)

// BPHandler listens to SQS for new messages describing freshly uploaded event data in S3.
type BPHandler struct {
	// Router process events, outputting metadata as files.
	Router *processor.EventRouter

	// Downloader downloads files from S3
	Downloader s3manageriface.DownloaderAPI
}

// NontrackedLogMessage is an SQS mesage containing data about the table the event should go into as
// well as the location (key) in S3 with event data.
type NontrackedLogMessage struct {
	Tablename string
	Keyname   string
}

// Handle an SQS message by downloading and processing the event data the message describes.
func (handler *BPHandler) Handle(msg *sqs.Message) error {
	var rotatedMessage NontrackedLogMessage

	err := json.Unmarshal([]byte(aws.StringValue(msg.Body)), &rotatedMessage)
	if err != nil {
		return fmt.Errorf("Could not decode %s\n", aws.StringValue(msg.Body))
	}

	tmpFile, err := ioutil.TempFile("", "schema_suggestor")
	if err != nil {
		return fmt.Errorf("Failed to create a tempfile to download %s: %v", rotatedMessage.Keyname, err)
	}
	defer func() {
		err = os.Remove(tmpFile.Name())
		if err != nil {
			log.Printf("Error removing file %s: %v", tmpFile.Name(), err)
		}
	}()
	log.Printf("Downloading %s into %s", rotatedMessage.Keyname, tmpFile.Name())

	parts := strings.SplitN(rotatedMessage.Keyname, "/", 2)
	n, err := handler.Downloader.Download(tmpFile, &s3.GetObjectInput{
		Bucket: aws.String(parts[0]),
		Key:    aws.String(parts[1]),
	})

	if err != nil {
		return fmt.Errorf("Error downloading %s into %s: %v", rotatedMessage.Keyname, tmpFile.Name(), err)
	}

	log.Printf("Downloaded a %d byte file\n", n)

	return handler.Router.ReadFile(tmpFile.Name())
}

func main() {
	flag.Parse()
	if *nonTrackedQueue == "" {
		log.Fatal("Missing required flag: --nonTrackedQueue.")
	}
	scoopClient := cachingscoopclient.New(*scoopURL, *transformConfig)

	// SQS listener pools SQS queue and then kicks off a jobs to
	// suggest the schemas.

	session := session.New()
	sqs := sqs.New(session)

	poller := listener.BuildSQSListener(
		&BPHandler{
			Router: processor.NewRouter(
				*staticFileDir,
				5*time.Minute,
				scoopClient,
			),
			Downloader: s3manager.NewDownloader(session),
		},
		2*time.Minute,
		sqs,
	)
	go poller.Listen(*nonTrackedQueue)

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

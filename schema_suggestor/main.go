package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
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
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/blueprint/bpdb"
	"github.com/twitchscience/blueprint/schema_suggestor/processor"
)

var (
	staticFileDir      = flag.String("staticfiles", "./static/events", "the location to serve static files from")
	bpdbConnection     = flag.String("bpdbConnection", "", "The connection string for blueprintdb")
	nonTrackedQueue    = flag.String("nonTrackedQueue", "", "SQS Queue name to listen to for nontracked events.")
	rollbarToken       = flag.String("rollbarToken", "", "Rollbar post_server_item token")
	rollbarEnvironment = flag.String("rollbarEnvironment", "", "Rollbar environment")
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
		return fmt.Errorf("could not decode %s", aws.StringValue(msg.Body))
	}

	tmpFile, err := ioutil.TempFile("", "schema_suggestor")
	if err != nil {
		return fmt.Errorf("failed to create a tempfile to download %s: %v", rotatedMessage.Keyname, err)
	}
	defer func() {
		err = os.Remove(tmpFile.Name())
		if err != nil {
			logger.WithError(err).WithField("tmp_file", tmpFile.Name()).Error("Failed to remove file")
		}
	}()
	logger.WithFields(map[string]interface{}{
		"key":      rotatedMessage.Keyname,
		"tmp_file": tmpFile.Name(),
	}).Debug("Downloading")

	parts := strings.SplitN(rotatedMessage.Keyname, "/", 2)
	n, err := handler.Downloader.Download(tmpFile, &s3.GetObjectInput{
		Bucket: aws.String(parts[0]),
		Key:    aws.String(parts[1]),
	})

	if err != nil {
		return fmt.Errorf("error downloading %s into %s: %v", rotatedMessage.Keyname, tmpFile.Name(), err)
	}
	logger.WithField("size_bytes", n).Debug("Downloaded")

	return handler.Router.ReadFile(tmpFile.Name())
}

func main() {
	flag.Parse()

	logger.InitWithRollbar("info", *rollbarToken, *rollbarEnvironment)
	logger.CaptureDefault()
	logger.Info("Starting!")
	defer logger.LogPanic()

	if *nonTrackedQueue == "" {
		logger.Fatal("Missing required flag: --nonTrackedQueue")
	}

	db, err := sql.Open("postgres", *bpdbConnection)
	if err != nil {
		logger.WithError(err).Fatal("Failed to connect to DB")
	}
	bpSchemaBackend := bpdb.NewSchemaBackend(db)

	// SQS listener pools SQS queue and then kicks off a jobs to
	// suggest the schemas.

	session, err := session.NewSession()
	if err != nil {
		logger.WithError(err).Fatalf("Error creating AWS session")
	}
	sqs := sqs.New(session, aws.NewConfig().WithMaxRetries(10))

	poller := listener.BuildSQSListener(
		&BPHandler{
			Router: processor.NewRouter(
				*staticFileDir,
				5*time.Minute,
				bpSchemaBackend,
			),
			Downloader: s3manager.NewDownloader(session),
		},
		2*time.Minute,
		sqs,
	)
	logger.Go(func() {
		poller.Listen(*nonTrackedQueue)
	})

	sigc := make(chan os.Signal, 1)
	wait := make(chan bool)

	signal.Notify(sigc, syscall.SIGINT)
	logger.Go(func() {
		<-sigc
		// Cause flush
		logger.Info("Sigint received -- shutting down")
		poller.Close()
		logger.Info("Exiting main cleanly.")
		logger.Wait()
		wait <- true
	})

	<-wait
}

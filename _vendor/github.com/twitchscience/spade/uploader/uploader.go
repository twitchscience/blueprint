package uploader

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/aws_utils/notifier"
	"github.com/twitchscience/aws_utils/uploader"
	gen "github.com/twitchscience/gologging/key_name_generator"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

// RedshiftSNSNotifierHarness is an SNS client that writes messages about uploaded event files
type RedshiftSNSNotifierHarness struct {
	topicARN string
	notifier *notifier.SNSClient
}

// SendMessage sends information to SNS about file uploaded to S3.
func (s *RedshiftSNSNotifierHarness) SendMessage(message *uploader.UploadReceipt) error {
	version, err := extractEventVersion(message.Path)
	if err != nil {
		return fmt.Errorf("extracting event version from path: %v", err)
	}

	err = s.notifier.SendMessage("uploadNotify", s.topicARN, extractEventName(message.Path), message.KeyName, version)
	if err != nil {
		return fmt.Errorf("sending Redshift SNS message: %v", err)
	}

	return nil
}

// BlueprintSNSNotifierHarness is an SNS client that writes messages about uploaded nontracked event files
type BlueprintSNSNotifierHarness struct {
	topicARN string
	notifier *notifier.SNSClient
}

// SendMessage sends information to SNS about file uploaded to S3.
func (s *BlueprintSNSNotifierHarness) SendMessage(message *uploader.UploadReceipt) error {
	if err := s.notifier.SendMessage("uploadNotify", s.topicARN, message.KeyName); err != nil {
		return fmt.Errorf("sending Blueprint SNS message: %v", err)
	}

	return nil
}

// NullNotifierHarness is a stub SNS client that does nothing.  In replay mode, we don't need to notify anyone of the
// newly uploaded files.
type NullNotifierHarness struct{}

// SendMessage is a noop
func (n *NullNotifierHarness) SendMessage(_ *uploader.UploadReceipt) error {
	return nil
}

func createSNSClient(sns snsiface.SNSAPI, f func(args ...interface{}) (*scoop_protocol.RowCopyRequest, error)) *notifier.SNSClient {
	client := notifier.BuildSNSClient(sns)
	client.Signer.RegisterMessageType("uploadNotify", func(args ...interface{}) (string, error) {
		message, err := f(args...)
		if err != nil {
			return "", fmt.Errorf("constructing RowCopyRequest: %v", err)
		}

		jsonMessage, err := json.Marshal(message)
		if err != nil {
			return "", fmt.Errorf("marshaling RowCopyRequest: %v", err)
		}
		return string(jsonMessage), nil
	})
	return client
}

func buildRedshiftNotifierHarness(sns snsiface.SNSAPI, topicARN string, replay bool) uploader.NotifierHarness {
	if replay {
		return &NullNotifierHarness{}
	}

	client := createSNSClient(sns, func(args ...interface{}) (*scoop_protocol.RowCopyRequest, error) {
		if len(args) != 3 {
			return nil, fmt.Errorf("expected 3 arguments, got %d", len(args))
		}

		var tableName, keyName string
		var tableVersion int
		var ok bool

		if tableName, ok = args[0].(string); !ok {
			return nil, fmt.Errorf("args[0] has type %T, expected string for table name", args[0])
		} else if keyName, ok = args[1].(string); !ok {
			return nil, fmt.Errorf("args[1] has type %T, expected string for key name", args[1])
		} else if tableVersion, ok = args[2].(int); !ok {
			return nil, fmt.Errorf("args[2] has type %T, expected int for table version", args[2])
		}

		return &scoop_protocol.RowCopyRequest{
			TableName:    tableName,
			KeyName:      keyName,
			TableVersion: tableVersion,
		}, nil
	})

	return &RedshiftSNSNotifierHarness{topicARN: topicARN, notifier: client}
}

func buildBlueprintNotifierHarness(sns snsiface.SNSAPI, topicARN string, replay bool) uploader.NotifierHarness {
	if replay {
		return &NullNotifierHarness{}
	}

	client := createSNSClient(sns, func(args ...interface{}) (*scoop_protocol.RowCopyRequest, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("expected 1 argument, got %d", len(args))
		}

		var keyName string
		var ok bool
		if keyName, ok = args[0].(string); !ok {
			return nil, fmt.Errorf("argument has type %T, expected string for key name", args[0])
		}

		return &scoop_protocol.RowCopyRequest{KeyName: keyName}, nil
	})

	return &BlueprintSNSNotifierHarness{topicARN: topicARN, notifier: client}
}

func extractEventName(filename string) string {
	path := strings.LastIndex(filename, "/") + 1
	ext := path + strings.Index(filename[path:], ".")
	if ext < 0 {
		ext = len(filename)
	}
	return filename[path:ext]
}

func extractEventVersion(filename string) (int, error) {
	path := strings.LastIndex(filename, ".v") + 2
	ext := strings.Index(filename, ".gz")
	if ext < 0 {
		ext = len(filename)
	}
	return strconv.Atoi(filename[path:ext])
}

// ProcessorErrorHandler sends messages about errors sending SNS messages to another topic.
type ProcessorErrorHandler struct {
	topicARN string
	notifier *notifier.SNSClient
}

// SendError sends the sending error to an topic.
func (p *ProcessorErrorHandler) SendError(err error) {
	logger.WithError(err).Error("Failed to send message to topic")
	if e := p.notifier.SendMessage("error", p.topicARN, err); e != nil {
		logger.WithError(e).Error("Failed to send error")
	}
}

// NullErrorHandler logs errors but does not send an SNS message.
type NullErrorHandler struct{}

// SendError logs the given error.
func (n *NullErrorHandler) SendError(err error) {
	logger.WithError(err).Error("Failed to send message to topic")
}

func buildErrorHandler(sns snsiface.SNSAPI, errorTopicArn string, nullNotifier bool) uploader.ErrorNotifierHarness {
	if nullNotifier {
		return &NullErrorHandler{}
	}
	return &ProcessorErrorHandler{
		notifier: notifier.BuildSNSClient(sns),
		topicARN: errorTopicArn,
	}
}

// remove the file or log the error and move on
func removeOrLog(path string) {
	if err := os.Remove(path); err != nil {
		logger.WithError(err).WithField("path", path).Error("Failed to remove file")
	}
}

// SafeGzipUpload validates a file is a valid gzip file and then uploads it.
func SafeGzipUpload(uploaderPool *uploader.UploaderPool, path string) {
	if isValidGzip(path) {
		uploaderPool.Upload(&uploader.UploadRequest{
			Filename: path,
			FileType: uploader.Gzip,
		})
	} else {
		logger.WithField("path", path).Warn("Not a valid gzip file; removing")
		removeOrLog(path)
	}
}

// salvageData uses the utility gzrecover to recover partial data from gzip,
// overwriting the corrupted gzip file. Returns false if no data was recovered
// or there was an error, else true
func salvageData(path string) (bool, error) {
	cmd := exec.Command("gzrecover", "-p", path)
	var salvaged bytes.Buffer
	cmd.Stdout = &salvaged
	err := cmd.Run()
	if err != nil {
		return false, fmt.Errorf("running gzrecover: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return false, fmt.Errorf("creating file to overwrite with salvaged data: %v", err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			logger.WithField("path", path).WithError(cerr).Error("Failed to close salvaged file")
		}
	}()

	writer := gzip.NewWriter(f)
	defer func() {
		if cerr := writer.Close(); cerr != nil {
			logger.WithField("path", path).WithError(cerr).Error("Failed to close salvaged data gzip writer")
		}
	}()

	// Writes (via gzip to file) all lines of the gzrecover output but the last,
	// as the last line likely was only partially written.
	writeSuccess := false
	for {
		bytes, err := salvaged.ReadBytes('\n')
		if err == io.EOF {
			return writeSuccess, nil
		} else if err != nil {
			return false, fmt.Errorf("reading from gzrecover output: %v", err)
		}

		if _, err = writer.Write(bytes); err != nil {
			return false, fmt.Errorf("writing salvaged data: %v", err)
		}
		writeSuccess = true
	}

}

func isValidGzip(path string) bool {
	entry := logger.WithField("path", path)
	file, err := os.Open(path)
	if err != nil {
		entry.WithError(err).Error("Failed to open")
		return false
	}
	defer func() {
		if err = file.Close(); err != nil {
			entry.WithError(err).Error("Failed to close file")
		}
	}()

	reader, err := gzip.NewReader(file)
	if err != nil {
		entry.WithError(err).Error("Failed to create gzip.NewReader")
		return false
	}
	defer func() {
		if err = reader.Close(); err != nil {
			entry.WithError(err).Error("Failed to close reader")
		}
	}()

	if _, err = ioutil.ReadAll(reader); err != nil {
		entry.WithError(err).Error("Failed to read gzipped file")
		return false
	}

	return true
}

func walkEventFiles(eventsDir string, f func(path string)) error {
	return filepath.Walk(eventsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && path != eventsDir {
			return filepath.SkipDir
		}
		if strings.HasSuffix(path, ".gz") {
			f(path)
		}
		return nil
	})
}

// ClearEventsFolder uploads all files in the eventsDir.
func ClearEventsFolder(uploaderPool *uploader.UploaderPool, eventsDir string) error {
	return walkEventFiles(eventsDir, func(path string) {
		SafeGzipUpload(uploaderPool, path)
	})
}

// SalvageCorruptedEvents salvages in place all invalid gzip files in the eventsDir
func SalvageCorruptedEvents(eventsDir string) error {
	return walkEventFiles(eventsDir, func(path string) {
		if isValidGzip(path) {
			return
		}

		salvaged, err := salvageData(path)
		if !salvaged {
			removeOrLog(path)
		}
		if err != nil {
			logger.WithField("path", path).WithError(err).Error("Failed to salvage data")
		}
	})
}

func buildInstanceInfo(replay bool) *gen.InstanceInfo {
	info := gen.BuildInstanceInfo(&gen.EnvInstanceFetcher{}, "spade_processor", "")
	if replay {
		info.Node = os.Getenv("HOST")
	}
	return info
}

type buildUploaderInput struct {
	bucketName       string
	topicARN         string
	errorTopicARN    string
	numWorkers       int
	sns              snsiface.SNSAPI
	s3Uploader       s3manageriface.UploaderAPI
	keyNameGenerator uploader.S3KeyNameGenerator
	nullNotifier     bool
	harness          uploader.NotifierHarness
}

func buildUploader(input *buildUploaderInput) *uploader.UploaderPool {
	return uploader.StartUploaderPool(
		input.numWorkers,
		buildErrorHandler(input.sns, input.errorTopicARN, input.nullNotifier),
		input.harness,
		uploader.NewFactory(input.bucketName, input.keyNameGenerator, input.s3Uploader),
	)
}

func redshiftKeyNameGenerator(info *gen.InstanceInfo, runTag string, replay bool) uploader.S3KeyNameGenerator {
	if replay {
		return &gen.ReplayKeyNameGenerator{Info: info, RunTag: runTag}
	}
	return &gen.ProcessorKeyNameGenerator{Info: info}
}

// BuildUploaderForRedshift builds an Uploader that uploads files to s3 and notifies sns.
func BuildUploaderForRedshift(numWorkers int, sns snsiface.SNSAPI, s3Uploader s3manageriface.UploaderAPI,
	aceBucketName, aceTopicARN, aceErrorTopicARN, runTag string, replay bool) *uploader.UploaderPool {

	harness := buildRedshiftNotifierHarness(sns, aceTopicARN, replay)
	return buildUploader(&buildUploaderInput{
		bucketName:       aceBucketName,
		topicARN:         aceTopicARN,
		errorTopicARN:    aceErrorTopicARN,
		numWorkers:       numWorkers,
		sns:              sns,
		s3Uploader:       s3Uploader,
		keyNameGenerator: redshiftKeyNameGenerator(buildInstanceInfo(replay), runTag, replay),
		nullNotifier:     replay,
		harness:          harness,
	})
}

// BuildUploaderForBlueprint builds an Uploader that uploads non-tracked events to s3 and notifies sns.
func BuildUploaderForBlueprint(numWorkers int, sns snsiface.SNSAPI, s3Uploader s3manageriface.UploaderAPI,
	nonTrackedBucketName, nonTrackedTopicARN, nonTrackedErrorTopicARN string, replay bool) *uploader.UploaderPool {

	harness := buildBlueprintNotifierHarness(sns, nonTrackedTopicARN, replay)
	return buildUploader(&buildUploaderInput{
		bucketName:       nonTrackedBucketName,
		topicARN:         nonTrackedTopicARN,
		errorTopicARN:    nonTrackedErrorTopicARN,
		numWorkers:       numWorkers,
		sns:              sns,
		s3Uploader:       s3Uploader,
		keyNameGenerator: &gen.EdgeKeyNameGenerator{Info: buildInstanceInfo(replay)},
		nullNotifier:     replay,
		harness:          harness,
	})
}

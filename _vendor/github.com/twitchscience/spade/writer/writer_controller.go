package writer

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/reporter"
)

const (
	inboundChannelBuffer = 400000
)

var (
	maxNonTrackedLogSize        = getInt64FromEnv("MAX_UNTRACKED_LOG_BYTES", 1<<29)                                 // default 500MB
	maxNonTrackedLogTimeAllowed = time.Duration(getInt64FromEnv("MAX_UNTRACKED_LOG_AGE_SECS", 10*60)) * time.Second // default 10 mins

	// EventsDir is the local subdirectory where successfully-transformed events are written.
	EventsDir = "events"
	// NonTrackedDir is the local subdirectory where non-tracked events are written.
	NonTrackedDir = "nontracked"
)

func getInt64FromEnv(target string, def int64) int64 {
	env := os.Getenv(target)
	if env == "" {
		return def
	}
	i, err := strconv.ParseInt(env, 10, 64)
	if err != nil {
		return def
	}
	return i
}

// SpadeWriter is an interface for writing to external sinks, like S3 or Kinesis.
type SpadeWriter interface {
	Write(*WriteRequest)
	Close() error

	// Rotate requests a rotation from the SpadeWriter, which *may* write to S3 or Kinesis depending on timing and
	// amount of information already buffered.  This should be called periodically, as this is the only time a
	// SpadeWriter will write to its sink (except on Close).  It returns a bool indicating whether all sinks were
	// written to and one of the errors which arose in writing (if any).
	Rotate() (bool, error)
}

type rotateResult struct {
	allDone bool // did the rotation close all the files
	err     error
}

type writerController struct {
	SpadeFolder       string
	Routes            map[string]SpadeWriter
	Reporter          reporter.Reporter
	redshiftUploader  *uploader.UploaderPool
	blueprintUploader *uploader.UploaderPool
	// The writer for the untracked events.
	NonTrackedWriter SpadeWriter

	inbound    chan *WriteRequest
	closeChan  chan error
	rotateChan chan chan rotateResult

	maxLogBytes   int64
	maxLogAgeSecs int64
}

// NewWriterController returns a writerController that handles logic to distribute
// writes across a number of workers.
// Each worker owns and operates one file. There are several sets of workers.
// Each set corresponds to a event type. Thus if we are processing a log
// file with 2 types of events we should produce (nWriters * 2) files
func NewWriterController(
	folder string,
	reporter reporter.Reporter,
	spadeUploaderPool *uploader.UploaderPool,
	blueprintUploaderPool *uploader.UploaderPool,
	maxLogBytes int64,
	maxLogAgeSecs int64,
) (SpadeWriter, error) {
	c := &writerController{
		SpadeFolder:       folder,
		Routes:            make(map[string]SpadeWriter),
		Reporter:          reporter,
		redshiftUploader:  spadeUploaderPool,
		blueprintUploader: blueprintUploaderPool,

		inbound:    make(chan *WriteRequest, inboundChannelBuffer),
		closeChan:  make(chan error),
		rotateChan: make(chan chan rotateResult),

		maxLogBytes:   maxLogBytes,
		maxLogAgeSecs: maxLogAgeSecs,
	}
	err := c.initNonTrackedWriter()
	if err != nil {
		return nil, err
	}

	logger.Go(c.Listen)
	return c, nil
}

// we put the event name in twice so that everything has a
// common prefix when we upload to s3
func getFilename(path, writerCategory string) string {
	return fmt.Sprintf("%s/%s.gz", path, writerCategory)
}

func (c *writerController) Write(req *WriteRequest) {
	c.inbound <- req
}

// TODO better Error handling
func (c *writerController) Listen() {
	for {
		select {
		case send := <-c.rotateChan:
			send <- c.rotate()
		case req, ok:= <-c.inbound:
			if !ok {
				c.closeChan <- c.close()
				return
			}

			if err := c.route(req); err != nil {
				logger.WithError(err).Error("Error routing write request")
			}
		}
	}
}

func (c *writerController) route(request *WriteRequest) error {
	switch request.Failure {
	// Success case
	case reporter.None, reporter.SkippedColumn:
		category := request.GetCategory()
		if _, hasWriter := c.Routes[category]; !hasWriter {
			newWriter, err := NewGzipWriter(
				c.SpadeFolder,
				EventsDir,
				category,
				c.Reporter,
				c.redshiftUploader,
				RotateConditions{
					MaxLogSize:     c.maxLogBytes,
					MaxTimeAllowed: time.Duration(c.maxLogAgeSecs) * time.Second,
				},
			)
			if err != nil {
				return err
			}
			c.Routes[category] = newWriter
		}
		c.Routes[category].Write(request)

	// Log non tracking events for blueprint
	case reporter.NonTrackingEvent:
		c.NonTrackedWriter.Write(request)

	// Otherwise tell the reporter that we got the event but it failed somewhere.
	default:
		c.Reporter.Record(request.GetResult())
	}
	return nil
}

func (c *writerController) initNonTrackedWriter() error {
	w, err := NewGzipWriter(
		c.SpadeFolder,
		NonTrackedDir,
		"nontracked",
		c.Reporter,
		c.blueprintUploader,
		RotateConditions{
			MaxLogSize:     maxNonTrackedLogSize,
			MaxTimeAllowed: maxNonTrackedLogTimeAllowed,
		},
	)
	if err != nil {
		return err
	}
	c.NonTrackedWriter = w
	return nil
}

func (c *writerController) Close() error {
	close(c.inbound)
	return <-c.closeChan
}

func (c *writerController) close() error {
	for _, w := range c.Routes {
		err := w.Close()
		if err != nil {
			return err
		}
	}

	return c.NonTrackedWriter.Close()
}

func (c *writerController) Rotate() (bool, error) {
	receive := make(chan rotateResult)
	defer close(receive)
	c.rotateChan <- receive
	result := <-receive
	return result.allDone, result.err
}

func (c *writerController) rotate() rotateResult {
	for k, w := range c.Routes {
		rotated, err := w.Rotate()
		if err != nil {
			return rotateResult{false, err}
		}
		if rotated {
			delete(c.Routes, k)
		}
	}

	rotated, err := c.NonTrackedWriter.Rotate()
	if err != nil {
		return rotateResult{false, err}
	}

	if rotated {
		if err = c.initNonTrackedWriter(); err != nil {
			return rotateResult{false, err}
		}
	}

	return rotateResult{rotated && len(c.Routes) == 0, err}
}

// MakeErrorRequest returns a WriteRequest indicating panic happened during processing.
func MakeErrorRequest(e *parser.MixpanelEvent, err interface{}) *WriteRequest {
	return &WriteRequest{
		Category: "Unknown",
		Line:     "",
		UUID:     "error",
		Source:   e.Properties,
		Failure:  reporter.PanickedInProcessing,
		Pstart:   e.Pstart,
	}
}

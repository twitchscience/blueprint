package scoop_protocol

// KinesisWriterConfig is used to configure a KinesisWriter
import (
	"errors"
	"fmt"
	"time"
)

// AnnotatedKinesisConfig is a Kinesis configuration annotated with meta information.
type AnnotatedKinesisConfig struct {
	AWSAccount       int64
	Team             string
	Version          int
	Contact          string
	Usage            string
	ConsumingLibrary string
	SpadeConfig      KinesisWriterConfig
	LastEditedAt     time.Time
	LastChangedBy    string
	Dropped          bool
	DroppedReason    string
}

// KinesisWriterEventConfig describes how a given Event is written to a Kinesis stream.
type KinesisWriterEventConfig struct {
	Filter       string
	FilterFunc   func(map[string]string) bool `json:"-"`
	Fields       []string
	FieldRenames map[string]string
	FullFieldMap map[string]string `json:"-"`
}

// KinesisWriterConfig describes a Kinesis Writer that the processor uses to export data to a Kinesis Stream/Firehose
// Make sure to call Validate() on Spade after loading this from JSON to populate some derived fields.
type KinesisWriterConfig struct {
	StreamName             string
	StreamRole             string
	StreamType             string // StreamType should be either "stream" or "firehose"
	Compress               bool   // true if compress data with flate, false to output json
	FirehoseRedshiftStream bool   // true if JSON destined for Firehose->Redshift streaming
	EventNameTargetField   string // Field name to write the event's name to (useful for uncompressed streams)
	ExcludeEmptyFields     bool   // true if empty fields should be excluded from the JSON
	BufferSize             int
	MaxAttemptsPerRecord   int
	RetryDelay             string

	Events map[string]*KinesisWriterEventConfig

	Globber GlobberConfig
	Batcher BatcherConfig
}

// Validate returns an error if the Kinesis Writer config is not valid, or nil if it is.
// It also sets the FilterFunc on Events with Filters and populates FullFieldMap.
func (c *KinesisWriterConfig) Validate() error {
	if c.StreamType == "" || c.StreamName == "" {
		return fmt.Errorf("Mandatory fields stream type and stream name aren't populated")
	}

	err := c.Globber.Validate()
	if err != nil {
		return fmt.Errorf("globber config invalid: %s", err)
	}

	err = c.Batcher.Validate()
	if err != nil {
		return fmt.Errorf("batcher config invalid: %s", err)
	}

	for _, e := range c.Events {
		if e.Filter != "" {
			e.FilterFunc = filterFuncs[e.Filter]
			if e.FilterFunc == nil {
				return fmt.Errorf("batcher config invalid: %s", err)
			}
		}
		e.FullFieldMap = make(map[string]string, len(e.Fields))
		if e.FieldRenames == nil {
			e.FieldRenames = make(map[string]string)
		}
		for _, f := range e.Fields {
			if renamed, ok := e.FieldRenames[f]; ok {
				e.FullFieldMap[f] = renamed
			} else {
				e.FullFieldMap[f] = f
			}
		}
	}

	if c.FirehoseRedshiftStream && (c.StreamType != "firehose" || c.Compress) {
		return fmt.Errorf("Redshift streaming only valid with non-compressed firehose")
	}

	_, err = time.ParseDuration(c.RetryDelay)
	return err
}

var filterFuncs = map[string]func(map[string]string) bool{
	"isVod": func(fields map[string]string) bool {
		return fields["vod_id"] != "" && fields["vod_type"] != "clip"
	},
	"isUserIDSet": func(fields map[string]string) bool {
		return fields["user_id"] != ""
	},
	"isChannelIDSet": func(fields map[string]string) bool {
		return fields["channel_id"] != ""
	},
}

// BatcherConfig is used to configure a batcher instance
type BatcherConfig struct {
	// MaxSize is the max combined size of the batch
	MaxSize int

	// MaxEntries is the max number of entries that can be batched together
	// if batches does not have an entry limit, set MaxEntries as -1
	MaxEntries int

	// MaxAge is the max age of the oldest entry in the glob
	MaxAge string

	// BufferLength is the length of the channel where newly
	// submitted entries are stored, decreasing the size of this
	// buffer can cause stalls, and increasing the size can increase
	// shutdown time
	BufferLength int
}

// Validate returns an error if the batcher config is invalid, nil otherwise.
func (c *BatcherConfig) Validate() error {
	maxAge, err := time.ParseDuration(c.MaxAge)
	if err != nil {
		return err
	}

	if maxAge <= 0 {
		return errors.New("MaxAge must be a positive value")
	}

	if c.MaxSize <= 0 {
		return errors.New("MaxSize must be a positive value")
	}

	if c.MaxEntries <= 0 && c.MaxEntries != -1 {
		return errors.New("MaxEntries must be a positive value or -1")
	}

	if c.BufferLength == 0 {
		return errors.New("BufferLength must be a positive value")
	}

	return nil
}

// GlobberConfig is used to configure a globber instance
type GlobberConfig struct {
	// MaxSize is the max size per glob before compression
	MaxSize int

	// MaxAge is the max age of the oldest entry in the glob
	MaxAge string

	// BufferLength is the length of the channel where newly
	// submitted entries are stored, decreasing the size of this
	// buffer can cause stalls, and increasing the size can increase
	// shutdown time
	BufferLength int
}

// Validate returns an error if the config is invalid, nil otherwise.
func (c *GlobberConfig) Validate() error {
	maxAge, err := time.ParseDuration(c.MaxAge)
	if err != nil {
		return err
	}

	if maxAge <= 0 {
		return errors.New("MaxAge must be a positive value")
	}

	if c.MaxSize <= 0 {
		return errors.New("MaxSize must be a positive value")
	}

	if c.BufferLength == 0 {
		return errors.New("BufferLength must be a positive value")
	}

	return nil
}

package scoop_protocol

// KinesisWriterConfig is used to configure a KinesisWriter
import (
	"errors"
	"fmt"
	"time"
)

// AnnotatedKinesisConfig is a Kinesis configuration annotated with meta information.
type AnnotatedKinesisConfig struct {
	ID               int
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
	Filter           string
	FilterFunc       func(map[string]string) bool `json:"-"`
	Fields           []string
	FieldRenames     map[string]string
	FullFieldMap     map[string]string `json:"-"`
	FilterParameters []*KinesisEventFilterConfig
}

// FilterOperator represents the types of filter operations supported by KinesisEventFilterConfig.
type FilterOperator string

const (
	IN_SET     FilterOperator = "in_set"
	NOT_IN_SET FilterOperator = "not_in_set"
)

// KinesisEventFilterConfig represents field/values that will be used to filter Events
// written to a Kinesis stream.
type KinesisEventFilterConfig struct {
	Field    string
	Values   []string
	Operator FilterOperator
}

// KinesisWriterConfig describes a Kinesis Writer that the processor uses to export data to a Kinesis Stream/Firehose
// Make sure to call Validate() on Spade after loading this from JSON to populate some derived fields.
type KinesisWriterConfig struct {
	StreamName             string
	StreamRole             string
	StreamType             string // StreamType should be either "stream" or "firehose"
	StreamRegion           string // AWS region to write to. Blank to use default region.
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

var allowedRegions = map[string]struct{}{
	"us-east-1": {},
	"us-west-2": {},
}

// Match returns true if the fieldValue matches the filter condition.
func (f *KinesisEventFilterConfig) Match(fieldValue string) bool {
	inSet := false
	for _, filterValue := range f.Values {
		if filterValue == fieldValue {
			inSet = true
			break
		}
	}
	return (!inSet && f.Operator == NOT_IN_SET) || (inSet && f.Operator == IN_SET)
}

// Validate returns an error if the Kinesis Writer config is not valid, or nil if it is.
// It also sets the FilterFunc on Events with Filters and populates FullFieldMap.
func (c *KinesisWriterConfig) Validate() error {
	if c.StreamType == "" || c.StreamName == "" {
		return fmt.Errorf("Mandatory fields stream type and stream name aren't populated")
	}

	err := c.Globber.Validate()
	if err != nil {
		return fmt.Errorf("globber config invalid: %v", err)
	}

	err = c.Batcher.Validate()
	if err != nil {
		return fmt.Errorf("batcher config invalid: %v", err)
	}

	if _, ok := allowedRegions[c.StreamRegion]; c.StreamRegion != "" && !ok {
		return fmt.Errorf("invalid region: %s", c.StreamRegion)
	}

	for name, e := range c.Events {
		if e.Filter != "" {
			filterGenerator := filterFuncGenerators[e.Filter]
			if filterGenerator != nil {
				if len(e.FilterParameters) < 1 {
					return fmt.Errorf("no filter parameters provided for event %s", name)
				}
				for _, filter := range e.FilterParameters {
					if len(filter.Field) < 1 {
						return fmt.Errorf("no field provided in filter params: %v", filter)
					}
					if len(filter.Values) < 1 {
						return fmt.Errorf("no values provided in filter params: %v", filter)
					}
					if filter.Operator != IN_SET && filter.Operator != NOT_IN_SET {
						return fmt.Errorf("no valid operator provided in filter params: %v", filter)
					}
				}
				e.FilterFunc = filterGenerator(e.FilterParameters)
			} else {
				e.FilterFunc = filterFuncs[e.Filter]
				if e.FilterFunc == nil {
					return fmt.Errorf("unknown filter: %s", e.Filter)
				}
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

// EventFilterFunc takes event properties and returns True if their values match desired conditions.
type EventFilterFunc func(map[string]string) bool

// generateEventFilterFunc takes a list of KinesisEventFilterConfigs to generate a closure
// that can be used to filter events by their field values.
func generateEventFilterFunc(filters []*KinesisEventFilterConfig) EventFilterFunc {
	filtersCopy := make([]*KinesisEventFilterConfig, len(filters))
	copy(filtersCopy, filters)
	return func(fields map[string]string) bool {
		for _, filter := range filtersCopy {
			fieldValue := fields[filter.Field]
			if !filter.Match(fieldValue) {
				return false
			}
		}
		return true
	}
}

// filterFuncGenerators represents functions for which the filters need to be provided as an argument.
var filterFuncGenerators = map[string]func([]*KinesisEventFilterConfig) EventFilterFunc{
	"isOneOf": func(filters []*KinesisEventFilterConfig) EventFilterFunc {
		return generateEventFilterFunc(filters)
	},
}

// filterFuncs represents functions for which filters are fixed in code.
var filterFuncs = map[string]EventFilterFunc{
	"isAGSEvent": func(fields map[string]string) bool {
		return generateEventFilterFunc([]*KinesisEventFilterConfig{
			&KinesisEventFilterConfig{
				Field: "adg_product_id",
				Values: []string{
					"600505cc-de2f-4b99-9960-c47ee5d23f04",
					"9233cb11-d375-4dd1-bd5e-b6d328fd5403",
				},
				Operator: IN_SET,
			},
		})(fields)
	},
	"isChannelIDSet": func(fields map[string]string) bool {
		return generateEventFilterFunc([]*KinesisEventFilterConfig{
			&KinesisEventFilterConfig{
				Field:    "channel_id",
				Values:   []string{""},
				Operator: NOT_IN_SET,
			},
		})(fields)
	},
	"isUserIDSet": func(fields map[string]string) bool {
		return generateEventFilterFunc([]*KinesisEventFilterConfig{
			&KinesisEventFilterConfig{
				Field:    "user_id",
				Values:   []string{""},
				Operator: NOT_IN_SET,
			},
		})(fields)
	},
	"isVod": func(fields map[string]string) bool {
		return generateEventFilterFunc([]*KinesisEventFilterConfig{
			&KinesisEventFilterConfig{
				Field:    "vod_id",
				Values:   []string{""},
				Operator: NOT_IN_SET,
			},
			&KinesisEventFilterConfig{
				Field:    "vod_type",
				Values:   []string{"clip"},
				Operator: NOT_IN_SET,
			},
		})(fields)
	},
	"isLiveClipContent": func(fields map[string]string) bool {
		return generateEventFilterFunc([]*KinesisEventFilterConfig{
			&KinesisEventFilterConfig{
				Field:    "source_content_type",
				Values:   []string{"live"},
				Operator: IN_SET,
			},
		})(fields)
	},
	"isTwilightApp": func(fields map[string]string) bool {
		return generateEventFilterFunc([]*KinesisEventFilterConfig{
			&KinesisEventFilterConfig{
				Field:    "client_app",
				Values:   []string{"twilight"},
				Operator: IN_SET,
			},
		})(fields)

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

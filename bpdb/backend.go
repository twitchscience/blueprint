package bpdb

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/scoop_protocol/transformer"
)

var (
	maxColumns = 300
	keyNames   = []string{"distkey", "sortkey"}
)

// AnnotatedSchema is a schema annotated with modification information.
type AnnotatedSchema struct {
	EventName     string
	Columns       []scoop_protocol.ColumnDefinition
	Version       int
	CreatedTS     time.Time
	TS            time.Time
	UserName      string
	Dropped       bool
	DropRequested bool
	Reason        string
}

// AnnotatedKinesisConfig is a Kinesis configuration annotated with meta information.
type AnnotatedKinesisConfig struct {
	StreamName       string
	StreamType       string
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

// ActiveUser is a count of the number of changes a user has made.
type ActiveUser struct {
	UserName string
	Changes  int
}

// DailyChange is a count of changes by users on a day.
type DailyChange struct {
	Day     string
	Changes int
	Users   int
}

// The following 3 structs and their validation functions are copypasta'd from processor.
// TODO: refactor them out of Blueprint/Processor and into Scoop Protocol

// KinesisWriterConfig is used to configure a KinesisWriter
// and its nested globber and batcher objects
type KinesisWriterConfig struct {
	StreamName             string
	StreamRole             string
	StreamType             string // StreamType should be either "stream" or "firehose"
	Compress               bool   // true if compress data with flate, false to output json
	FirehoseRedshiftStream bool   // true if JSON destined for Firehose->Redshift streaming
	BufferSize             int
	MaxAttemptsPerRecord   int
	RetryDelay             string

	Events map[string]*struct {
		Filter     string
		FilterFunc func(map[string]string) bool `json:"-"`
		Fields     []string
	}

	Globber GlobberConfig
	Batcher BatcherConfig
}

// Validate returns an error if the Kinesis Writer config is not valid, or nil if it is.
// It also sets the FilterFunc on Events with Filters.
func (c *KinesisWriterConfig) Validate() error {
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

// Bpdb is the interface of the blueprint db backend that interacts with maintenance mode and stats
type Bpdb interface {
	IsInMaintenanceMode() bool
	SetMaintenanceMode(switchingOn bool, reason string) error
	ActiveUsersLast30Days() ([]*ActiveUser, error)
	DailyChangesLast30Days() ([]*DailyChange, error)
}

// BpSchemaBackend is the interface of the blueprint db backend that stores schema state
type BpSchemaBackend interface {
	AllSchemas() ([]AnnotatedSchema, error)
	Schema(name string) (*AnnotatedSchema, error)
	UpdateSchema(update *core.ClientUpdateSchemaRequest, user string) *core.WebError
	CreateSchema(schema *scoop_protocol.Config, user string) *core.WebError
	Migration(table string, to int) ([]*scoop_protocol.Operation, error)
	DropSchema(schema *AnnotatedSchema, reason string, exists bool, user string) error
}

// BpKinesisConfigBackend is the interface of the blueprint db backend that stores kinesis config state
type BpKinesisConfigBackend interface {
	AllKinesisConfigs() ([]AnnotatedKinesisConfig, error)
	KinesisConfig(account int64, streamType string, name string) (*AnnotatedKinesisConfig, error)
	UpdateKinesisConfig(update *AnnotatedKinesisConfig, user string) *core.WebError
	CreateKinesisConfig(config *AnnotatedKinesisConfig, user string) *core.WebError
	DropKinesisConfig(config *AnnotatedKinesisConfig, reason string, user string) error
}

func validateType(t string) error {
	for _, validType := range transformer.ValidTransforms {
		if validType == t {
			return nil
		}
	}
	return fmt.Errorf("type not found")
}

func validateIdentifier(name string) error {
	if len(name) < 1 || len(name) > 127 {
		return fmt.Errorf("must be between 1 and 127 characters, given length of %d", len(name))
	}
	matched, _ := regexp.MatchString(`^[A-Za-z_][A-Za-z0-9_-]*$`, name)
	if !matched {
		return fmt.Errorf("must begin with alpha or underscore and be composed of alphanumeric, underscore, or hyphen")
	}
	return nil
}

func validateIsNotKey(options string) error {
	for _, keyName := range keyNames {
		if strings.Contains(options, keyName) {
			return fmt.Errorf("this column is %s", keyName)
		}
	}
	return nil
}

func preValidateSchema(schema *scoop_protocol.Config) error {
	err := validateIdentifier(schema.EventName)
	if err != nil {
		return fmt.Errorf("event name invalid: %v", err)
	}
	for _, col := range schema.Columns {
		err = validateIdentifier(col.OutboundName)
		if err != nil {
			return fmt.Errorf("column outbound name invalid: %v", err)
		}
		err = validateType(col.Transformer)
		if err != nil {
			return fmt.Errorf("column transformer invalid: %v", err)
		}
	}
	ops := schemaCreateRequestToOps(schema)
	err = ApplyOperations(&AnnotatedSchema{}, ops)
	if err != nil {
		return err
	}
	if len(schema.Columns) >= maxColumns {
		return fmt.Errorf("too many columns, max is %d, given %d", maxColumns, len(schema.Columns))
	}
	return nil
}

// schemaCreateRequestToOps converts a schema update request into a list of add operations
func schemaCreateRequestToOps(req *scoop_protocol.Config) []scoop_protocol.Operation {
	ops := make([]scoop_protocol.Operation, 0, len(req.Columns))
	for _, col := range req.Columns {
		ops = append(ops, scoop_protocol.NewAddOperation(col.OutboundName, col.InboundName,
			col.Transformer, col.ColumnCreationOptions, col.SupportingColumns))
	}
	return ops
}

// schemaUpdateRequestToOps converts a schema update request into a list of operations
func schemaUpdateRequestToOps(req *core.ClientUpdateSchemaRequest) []scoop_protocol.Operation {
	ops := make([]scoop_protocol.Operation, 0, len(req.Additions)+len(req.Deletes)+len(req.Renames))
	for _, colName := range req.Deletes {
		ops = append(ops, scoop_protocol.NewDeleteOperation(colName))
	}
	for _, col := range req.Additions {
		ops = append(ops, scoop_protocol.NewAddOperation(col.OutboundName, col.InboundName,
			col.Transformer, col.Length, col.SupportingColumns))
	}
	for oldName, newName := range req.Renames {
		ops = append(ops, scoop_protocol.NewRenameOperation(oldName, newName))
	}
	return ops
}

func preValidateUpdate(req *core.ClientUpdateSchemaRequest, schema *AnnotatedSchema) string {
	if schema.DropRequested || schema.Dropped {
		return "Attempted to modify drop-requested/dropped schema"
	}

	columnDefs := make(map[string]*scoop_protocol.ColumnDefinition)
	for _, col := range schema.Columns {
		columnDefs[col.OutboundName] = &col
	}

	// Validate schema "delete"s
	for _, columnName := range req.Deletes {
		existingCol, exists := columnDefs[columnName]
		if !exists {
			return fmt.Sprintf("Attempting to delete column that doesn't exist: %s", columnName)
		}
		err := validateIsNotKey(existingCol.ColumnCreationOptions)
		if err != nil {
			return fmt.Sprintf("Column is a key and cannot be dropped: %s", columnName)
		}
		delete(columnDefs, columnName)
	}

	// Validate schema "add"s
	for _, col := range req.Additions {
		err := validateIdentifier(col.OutboundName)
		if err != nil {
			return fmt.Sprintf("Column outbound name invalid: %v", err)
		}
		err = validateType(col.Transformer)
		if err != nil {
			return fmt.Sprintf("Column transformer invalid: %v", err)
		}
		_, exists := columnDefs[col.OutboundName]
		if exists {
			return fmt.Sprintf("Attempting to add duplicate column: %s", col.OutboundName)
		}
		columnDefs[col.OutboundName] = nil
	}

	renameSet := make(map[string]bool)
	// Validate schema "rename"s
	for oldName, newName := range req.Renames {
		err := validateIdentifier(newName)
		if err != nil {
			return fmt.Sprintf("New name for column is invalid: %v", err)
		}
		_, exists := columnDefs[oldName]
		if !exists {
			return fmt.Sprintf("Attempting to rename column that doesn't exist: %s", oldName)
		}
		for _, name := range []string{oldName, newName} {
			_, found := renameSet[name]
			if found {
				return fmt.Sprintf("Cannot rename from or to a column that was already renamed from or to. Offending name: %v", name)
			}
			renameSet[name] = true
		}
	}
	// Do this in a separate loop because the message about renaming to/from a column is better.
	for _, newName := range req.Renames {
		_, exists := columnDefs[newName]
		if exists {
			return fmt.Sprintf("Attempting to rename to duplicate column: %s", newName)
		}
	}

	if len(schema.Columns) > maxColumns {
		return fmt.Sprintf(
			"too many columns, max is %d, given %d adds and %d deletes, which would result in %d total",
			maxColumns, len(req.Additions), len(req.Deletes), len(schema.Columns))
	}
	return ""
}

func validateStreamType(t string) error {
	if t != "stream" && t != "firehose" {
		return fmt.Errorf("type not found")
	}
	return nil
}

func validateKinesisConfig(config *AnnotatedKinesisConfig) error {
	err := validateIdentifier(config.StreamName)
	if err != nil {
		return fmt.Errorf("stream name invalid: %v", err)
	}
	err = validateStreamType(config.StreamType)
	if err != nil {
		return fmt.Errorf("stream type invalid: %v", err)
	}
	err = validateStreamType(config.SpadeConfig.StreamType)
	if err != nil {
		return fmt.Errorf("stream type invalid: %v", err)
	}
	if config.StreamName != config.SpadeConfig.StreamName {
		return fmt.Errorf("stream name annotation does not match JSON")
	}
	if config.StreamType != config.SpadeConfig.StreamType {
		return fmt.Errorf("stream type annotation does not match JSON")
	}
	err = config.SpadeConfig.Validate()
	if err != nil {
		return fmt.Errorf("Kinesis stream internal validate failed: %v", err)
	}
	return nil
}

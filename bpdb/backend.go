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
	maxColumns               = 300
	keyNames                 = []string{"distkey", "sortkey"}
	blacklistedOutboundNames = []string{"date"}
	timeColName              = "time"
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

// EventComment is the comment associated with an event schema
type EventComment struct {
	EventName string
	Comment   string
	TS        time.Time
	UserName  string
	Version   int
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
	AllKinesisConfigs() ([]scoop_protocol.AnnotatedKinesisConfig, error)
	KinesisConfig(account int64, streamType string, name string) (*scoop_protocol.AnnotatedKinesisConfig, error)
	UpdateKinesisConfig(update *scoop_protocol.AnnotatedKinesisConfig, user string) *core.WebError
	CreateKinesisConfig(config *scoop_protocol.AnnotatedKinesisConfig, user string) *core.WebError
	DropKinesisConfig(config *scoop_protocol.AnnotatedKinesisConfig, reason string, user string) error
}

// BpEventCommentBackend is the interface of the blueprint db backend that stores event comment state
type BpEventCommentBackend interface {
	EventComment(name string) (*EventComment, error)
	UpdateEventComment(req *core.ClientUpdateEventCommentRequest, user string) *core.WebError
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

// stringInSlice returns true if the string is in the list of strings
func stringInSlice(needle string, haystack []string) bool {
	for _, a := range haystack {
		if a == needle {
			return true
		}
	}
	return false
}

func validateOutboundName(name string) error {
	if err := validateIdentifier(name); err != nil {
		return err
	}
	if stringInSlice(strings.ToLower(name), blacklistedOutboundNames) {
		return fmt.Errorf("column %s is a reserved OutboundName", name)
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

func validateHasTime(cols []scoop_protocol.ColumnDefinition) error {
	for _, col := range cols {
		if col.OutboundName == timeColName && col.InboundName == timeColName && col.Transformer == "f@timestamp@unix" {
			return nil
		}
	}
	return errors.New("Schema must contain time->time of type f@timestamp@unix")
}

func preValidateSchema(schema *scoop_protocol.Config) error {
	err := validateIdentifier(schema.EventName)
	if err != nil {
		return fmt.Errorf("event name invalid: %v", err)
	}
	for _, col := range schema.Columns {
		err = validateOutboundName(col.OutboundName)
		if err != nil {
			return fmt.Errorf("column outbound name invalid: %v", err)
		}
		err = validateType(col.Transformer)
		if err != nil {
			return fmt.Errorf("column transformer invalid: %v", err)
		}
	}
	err = validateHasTime(schema.Columns)
	if err != nil {
		return err
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
		if columnName == timeColName {
			return "Cannot delete time column."
		}
		delete(columnDefs, columnName)
	}

	// Validate schema "add"s
	for _, col := range req.Additions {
		err := validateOutboundName(col.OutboundName)
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
		err := validateOutboundName(newName)
		if err != nil {
			return fmt.Sprintf("New name for column is invalid: %v", err)
		}
		_, exists := columnDefs[oldName]
		if !exists {
			return fmt.Sprintf("Attempting to rename column that doesn't exist: %s", oldName)
		}
		if oldName == timeColName {
			return "Cannot rename time column"
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

func validateKinesisConfig(config *scoop_protocol.AnnotatedKinesisConfig) error {
	err := validateIdentifier(config.SpadeConfig.StreamName)
	if err != nil {
		return fmt.Errorf("stream name invalid: %v", err)
	}
	err = validateStreamType(config.SpadeConfig.StreamType)
	if err != nil {
		return fmt.Errorf("stream type invalid: %v", err)
	}
	err = validateStreamType(config.SpadeConfig.StreamType)
	if err != nil {
		return fmt.Errorf("stream type invalid: %v", err)
	}
	err = config.SpadeConfig.Validate()
	if err != nil {
		return fmt.Errorf("Kinesis stream internal validate failed: %v", err)
	}
	return nil
}

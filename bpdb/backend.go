package bpdb

import (
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

// Bpdb is the interface of the blueprint db backend that stores schema state
type Bpdb interface {
	AllSchemas() ([]AnnotatedSchema, error)
	Schema(name string) (*AnnotatedSchema, error)
	UpdateSchema(update *core.ClientUpdateSchemaRequest, user string) error
	CreateSchema(schema *scoop_protocol.Config, user string) error
	Migration(table string, to int) ([]*scoop_protocol.Operation, error)
	DropSchema(schema *AnnotatedSchema, reason string, exists bool, user string) error
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
		ops = append(ops, scoop_protocol.NewAddOperation(col.OutboundName, col.InboundName, col.Transformer, col.ColumnCreationOptions))
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
		ops = append(ops, scoop_protocol.NewAddOperation(col.OutboundName, col.InboundName, col.Transformer, col.Length))
	}
	for oldName, newName := range req.Renames {
		ops = append(ops, scoop_protocol.NewRenameOperation(oldName, newName))
	}
	return ops
}

func preValidateUpdate(req *core.ClientUpdateSchemaRequest, bpdb Bpdb) error {
	schema, err := bpdb.Schema(req.EventName)
	if err != nil {
		return fmt.Errorf("error getting schema to validate schema update: %v", err)
	}
	if schema.DropRequested || schema.Dropped {
		return fmt.Errorf("attempted to modify drop-requested/dropped schema")
	}

	// Validate schema "delete"s
	for _, columnName := range req.Deletes {
		for _, existingCol := range schema.Columns {
			if existingCol.OutboundName == columnName {
				err = validateIsNotKey(existingCol.ColumnCreationOptions)
				if err != nil {
					return fmt.Errorf("column is a key and cannot be dropped: %v", err)
				}
				break // move on to next deleted column
			}
		}
	}

	// Validate schema "add"s
	for _, col := range req.Additions {
		err = validateIdentifier(col.OutboundName)
		if err != nil {
			return fmt.Errorf("column outbound name invalid: %v", err)
		}
		err = validateType(col.Transformer)
		if err != nil {
			return fmt.Errorf("column transformer invalid: %v", err)
		}
	}

	// Validate schema "rename"s
	nameSet := make(map[string]bool)
	for oldName, newName := range req.Renames {
		err = validateIdentifier(newName)
		if err != nil {
			return fmt.Errorf("New name for column is invalid: %v", err)
		}
		for _, name := range []string{oldName, newName} {
			_, found := nameSet[name]
			if found {
				return fmt.Errorf("Cannot rename from or to a column that was already renamed from or to. Offending name: %v", name)
			}
			nameSet[name] = true
		}
	}

	ops := schemaUpdateRequestToOps(req)
	err = ApplyOperations(schema, ops)
	if err != nil {
		return err
	}

	if len(schema.Columns) > maxColumns {
		return fmt.Errorf("too many columns, max is %d, given %d adds and %d deletes, which would result in %d total", maxColumns, len(req.Additions), len(req.Deletes), len(schema.Columns))
	}
	return nil
}

package bpdb

import (
	"fmt"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

func addColumn(s *scoop_protocol.Config, col scoop_protocol.ColumnDefinition) error {
	for _, existingCol := range s.Columns {
		if existingCol.OutboundName == col.OutboundName {
			return fmt.Errorf("Outbound column '%s' already exists in schema, cannot add again.", col.OutboundName)
		}
	}
	s.Columns = append(s.Columns, col)
	return nil
}

// ApplyOperations applies the list of operations in order to the schema,
// migrating the schema to a new state
func ApplyOperations(s *scoop_protocol.Config, operations []Operation) error {
	for _, op := range operations {
		err := ApplyOperation(s, op)
		if err != nil {
			return err
		}
	}
	return nil
}

// ApplyOperation applies a single operation to the schema, migrating the
// schema to a new state
func ApplyOperation(s *scoop_protocol.Config, op Operation) error {
	switch op.action {
	case "add":
		err := addColumn(s, scoop_protocol.ColumnDefinition{
			InboundName:           op.inbound,
			OutboundName:          op.outbound,
			Transformer:           op.columnType,
			ColumnCreationOptions: op.columnOptions,
		})
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("Error, unsupported operation action %s.", op.action)
	}
	return nil
}

package bpdb

import (
	"fmt"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

// ApplyOperations applies the list of operations in order to the schema,
// migrating the schema to a new state
func ApplyOperations(s *AnnotatedSchema, operations []scoop_protocol.Operation) error {
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
func ApplyOperation(s *AnnotatedSchema, op scoop_protocol.Operation) error {
	switch op.Action {
	case scoop_protocol.ADD:
		for _, existingCol := range s.Columns {
			if existingCol.OutboundName == op.Name {
				return fmt.Errorf("Outbound column '%s' already exists in schema, cannot add again.", op.Name)
			}
		}
		s.Columns = append(s.Columns, scoop_protocol.ColumnDefinition{
			InboundName:           op.ActionMetadata["inbound"],
			OutboundName:          op.Name,
			Transformer:           op.ActionMetadata["column_type"],
			ColumnCreationOptions: op.ActionMetadata["column_options"],
		})
		s.Dropped = false
		s.DropRequested = false
		s.Reason = ""
	case scoop_protocol.DELETE:
		for i, existingCol := range s.Columns {
			if existingCol.OutboundName == op.Name {
				// splice the dropped column away
				s.Columns = append(s.Columns[:i], s.Columns[i+1:]...)
				return nil
			}
		}
		return fmt.Errorf("Outbound column '%s' does not exists in schema, cannot drop non-existing column.", op.Name)
	case scoop_protocol.RENAME:
		for i, existingCol := range s.Columns {
			if existingCol.OutboundName == op.Name {
				s.Columns[i].OutboundName = op.ActionMetadata["new_outbound"]
				return nil
			}
		}
		return fmt.Errorf("Outbound column '%s' does not exists in schema, cannot rename non-existent column.", op.Name)
	case scoop_protocol.REQUEST_DROP_EVENT:
		s.DropRequested = true
		s.Reason = op.ActionMetadata["reason"]
	case scoop_protocol.DROP_EVENT:
		s.DropRequested = false
		s.Dropped = true
		s.Columns = []scoop_protocol.ColumnDefinition{}
		if s.Reason == "" {
			s.Reason = op.ActionMetadata["reason"]
		}
	case scoop_protocol.CANCEL_DROP_EVENT:
		s.DropRequested = false
		s.Reason = ""
	default:
		return fmt.Errorf("Error, unsupported operation action %s.", op.Action)
	}
	return nil
}

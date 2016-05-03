package bpdb

import "fmt"

func (s *Schema) addColumn(col Column) error {
	for _, existingCol := range s.Columns {
		if existingCol.OutboundName == col.OutboundName {
			return fmt.Errorf("Outbound column '%s' already exists in schema, cannot add again.", col.OutboundName)
		}
	}
	s.Columns = append(s.Columns, col)
	return nil
}

func (s *Schema) ApplyOperations(operations []Operation) error {
	for _, op := range operations {
		err := s.ApplyOperation(op)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Schema) ApplyOperation(op Operation) error {
	switch op.action {
	case "add":
		err := s.addColumn(Column{op.inbound, op.outbound, op.column_type, op.column_options})
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("Error, unsupported operation action %s.", op.action)
	}
	return nil
}

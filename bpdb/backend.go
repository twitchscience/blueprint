package bpdb

import "github.com/twitchscience/blueprint/core"

// Operation represents a single change to a schema
type Operation struct {
	action         string
	inbound        string
	outbound       string
	column_type    string
	column_options string
}

type Column struct {
	InboundName           string
	OutboundName          string
	Transformer           string
	ColumnCreationOptions string
}

type Schema struct {
	EventName string
	Columns   []Column
}

type Bpdb interface {
	AllSchemas() ([]Schema, error)
	UpdateSchema(*core.ClientUpdateSchemaRequest) error
}

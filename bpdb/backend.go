package bpdb

import (
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

// Operation represents a single change to a schema
type Operation struct {
	action        string
	inbound       string
	outbound      string
	columnType    string
	columnOptions string
}

// Column represents a schema's column
type Column struct {
	InboundName           string
	OutboundName          string
	Transformer           string
	ColumnCreationOptions string
}

// Schema represents a single table/event
type Schema struct {
	EventName string
	Columns   []Column
}

// Bpdb is the interface of the blueprint db backend that stores schema state
type Bpdb interface {
	AllSchemas() ([]Schema, error)
	UpdateSchema(*core.ClientUpdateSchemaRequest) error
	CreateSchema(*scoop_protocol.Config) error
}

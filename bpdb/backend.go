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

// Bpdb is the interface of the blueprint db backend that stores schema state
type Bpdb interface {
	AllSchemas() ([]scoop_protocol.Config, error)
	Schema(name string) (*scoop_protocol.Config, error)
	UpdateSchema(*core.ClientUpdateSchemaRequest) error
	CreateSchema(*scoop_protocol.Config) error
	Migration(table string, to int) ([]*scoop_protocol.Operation, error)
}

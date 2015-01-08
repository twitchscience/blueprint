package core

import (
	"log"
	"sync"

	"github.com/twitchscience/scoop_protocol/schema"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

// Subprocess represents something that can be set up, started, and stopped. E.g. a server.
type Subprocess interface {
	Setup() error
	Start()
	Stop()
}

// SubprocessManager oversees multiple subprocesses.
type SubprocessManager struct {
	Processes []Subprocess
	wg        *sync.WaitGroup
}

// Start all subprocesses. If any return an error, exits the process.
func (s *SubprocessManager) Start() {
	var wg sync.WaitGroup
	for _, sp := range s.Processes {
		err := sp.Setup()
		if err != nil {
			log.Fatal("Failed to set up subprocess", sp, err)
		}

		fn := sp
		wg.Add(1)
		go func() {
			fn.Start()
			wg.Done()
		}()
	}
	s.wg = &wg
}

// Wait for all subprocesses to finish, i.e. return from their Start method.
func (s *SubprocessManager) Wait() {
	s.wg.Wait()
}

// Stop all subprocesses.
func (s *SubprocessManager) Stop() {
	for _, sp := range s.Processes {
		sp.Stop()
	}
}

// Column represents a SQL column in a table for event data.
type Column struct {
	// InboundName is the name of the event property.
	InboundName string `json:"InboundName"`

	// OutboundName is the name of the column for the event property.
	OutboundName string `json:"OutboundName"`

	// Transformer is the column's SQL type.
	Transformer string `json:"Transformer"`

	// Length is the length of the SQL type, e.g. for a variable type like varchar.
	// TODO: length should be an int, currently the client supplies this
	// to us, so pass through now, with a view to fixing this later
	Length string `json:"ColumnCreationOptions"`
}

// ClientUpdateSchemaRequest is a request to update the schema for an event.
type ClientUpdateSchemaRequest struct {
	EventName string `json:"-"`
	Columns   []Column
}

// ConvertToScoopRequest converts the Column data to scoop column definitions and returns a scoop update request.
func (c *ClientUpdateSchemaRequest) ConvertToScoopRequest() *schema.UpdateSchemaRequest {
	cols := make([]scoop_protocol.ColumnDefinition, 0, len(c.Columns))
	for _, c := range c.Columns {
		cols = append(cols, scoop_protocol.ColumnDefinition{
			InboundName:           c.InboundName,
			OutboundName:          c.OutboundName,
			Transformer:           c.Transformer,
			ColumnCreationOptions: makeColumnOpts(c.Length),
		})
	}
	return &schema.UpdateSchemaRequest{
		Columns: cols,
	}
}

// TODO: as above, the client shouldn't massage this as it is, once
// that is fixed this function will produce the necessary data
// structure, maybe a string, maybe something provided by scoop (I
// prefer the munging into a string to be in scoop).
func makeColumnOpts(clientProvided string) string {
	return clientProvided
}

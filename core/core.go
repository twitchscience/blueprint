package core

import (
	"sync"

	"github.com/twitchscience/aws_utils/logger"
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
			logger.WithError(err).WithField("subprocess", sp).Fatal("Failed to set up subprocess")
		}

		fn := sp
		wg.Add(1)
		logger.Go(func() {
			fn.Start()
			wg.Done()
		})
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

	// SupportingColumns are the names of extra columns required to map a value to this column
	SupportingColumns string `json:"SupportingColumns"`
}

// Renames is a map of old name to new name, representing a rename operation on
// a set of columns.
type Renames map[string]string

// ClientUpdateSchemaRequest is a request to update the schema for an event.
type ClientUpdateSchemaRequest struct {
	EventName string `json:"-"`
	Additions []Column
	Deletes   []string
	Renames   Renames
}

// ClientDropSchemaRequest is a request to drop the schema for an event.
type ClientDropSchemaRequest struct {
	EventName string
	Reason    string
}

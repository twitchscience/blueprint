package core

import (
	"log"
	"sync"

	"github.com/twitchscience/scoop_protocol/schema"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

type Subprocess interface {
	Setup() error
	Start()
	Stop()
}

type SubprocessManager struct {
	Processes []Subprocess
	wg        *sync.WaitGroup
}

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

func (s *SubprocessManager) Wait() {
	s.wg.Wait()
}

func (s *SubprocessManager) Stop() {
	for _, sp := range s.Processes {
		sp.Stop()
	}
}

// TODO: length should be an int, currently the client supplies this
// to us, so pass through now, with a view to fixing this later
type Column struct {
	InboundName  string `json:"InboundName"`
	OutboundName string `json:"OutboundName"`
	Transformer  string `json:"Transformer"`
	Length       string `json:"ColumnCreationOptions"`
}

type ClientUpdateSchemaRequest struct {
	EventName string `json:"-"`
	Columns   []Column
}

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

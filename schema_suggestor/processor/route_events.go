package processor

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"time"

	"github.com/twitchscience/blueprint/scoopclient"
)

type EventRouter struct {
	CurrentTables    []string
	Processors       map[string]EventProcessor
	ProcessorFactory func() EventProcessor
	FlushTimer       <-chan time.Time
	ScoopClient      scoopclient.ScoopClient
}

func NewRouter(
	outputDir string,
	flushInterval time.Duration,
	scoopClient scoopclient.ScoopClient,
) *EventRouter {
	r := &EventRouter{
		Processors: make(map[string]EventProcessor),
		ProcessorFactory: func() EventProcessor {
			return &NonTrackedEventProcessor{
				Out:    NewOutputter(outputDir),
				Events: make([]*PropertySummary, 0),
			}
		},
		FlushTimer: time.Tick(flushInterval),
	}
	r.UpdateCurrentTables()
	return r
}

type MPEvent struct {
	Event      string
	Properties map[string]interface{}
}

func (e *EventRouter) ReadFile(filename string) error {
	e.UpdateCurrentTables()

	file, err := os.Open(filename)
	if err != nil {
		return err
	}

	d := json.NewDecoder(file)
	for {
		var event MPEvent
		if err := d.Decode(&event); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		e.Route(event.Event, event.Properties)
	}
	// if the Ticker has a message in the channel then we flush. Otherwise continue...
	select {
	case <-e.FlushTimer:
		e.FlushRouters()
	default:
	}

	return nil
}

func (e *EventRouter) UpdateCurrentTables() {
	// talk to scoop...
	configs, err := e.ScoopClient.FetchAllSchemas()
	if err != nil {
		return
	}
	newTables := make([]string, len(configs))
	for idx, config := range configs {
		newTables[idx] = config.EventName
	}
	e.CurrentTables = newTables
}

func (e *EventRouter) Route(eventName string, properties map[string]interface{}) {
	if e.EventCreated(eventName) {
		return
	}

	if _, ok := e.Processors[eventName]; !ok {
		e.Processors[eventName] = e.ProcessorFactory()
	}
	go e.Processors[eventName].Accept(properties)
}

func (e *EventRouter) FlushRouters() {
	for event, processor := range e.Processors {
		processor.Flush(event)
	}
	// removed tracked events here (at least limit the time of the race duration)
}

func (e *EventRouter) EventCreated(eventName string) bool {
	for _, tables := range e.CurrentTables {
		if tables == eventName {
			return true
		}
	}
	return false
}

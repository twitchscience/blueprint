package processor

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/twitchscience/blueprint/scoopclient"
)

// EventRouter receives Mixpanel events, and for events that do not have a table yet, outputs files
// describing the table for that event.
type EventRouter struct {
	// CurrentTables maintains the current event names with schemas in scoop. It is updated periodically.
	CurrentTables []string

	// Processors aggregate data about different event types.
	Processors map[string]EventProcessor

	// ProcessorFactory creates a new Processor for a previously unseen event type.
	ProcessorFactory func(string) EventProcessor

	// FlushTimer will peridically flush data about events to the output directory.
	FlushTimer <-chan time.Time

	// ScoopClient talks to scoop to get the current tables.
	ScoopClient scoopclient.ScoopClient

	// GzipReader is for reading files, and is re-used.
	GzipReader *gzip.Reader

	// OutputDir to place files.
	OutputDir string
}

// NewRouter allocates a new EventRouter that outputs transformations to a given output directory.
func NewRouter(
	outputDir string,
	flushInterval time.Duration,
	scoopClient scoopclient.ScoopClient,
) *EventRouter {
	r := &EventRouter{
		Processors:       make(map[string]EventProcessor),
		ProcessorFactory: NewNonTrackedEventProcessor,
		FlushTimer:       time.Tick(flushInterval),
		ScoopClient:      scoopClient,
		OutputDir:        outputDir,
	}
	r.UpdateCurrentTables()
	return r
}

// MPEvent is a Mixpanel event.
type MPEvent struct {
	Event      string
	Properties map[string]interface{}
}

// ReadFile reads a file of Mixpanel events and routes them to event aggregators.
// If the flush interval has expired, it will flush all even aggregators after reading the file.
func (e *EventRouter) ReadFile(filename string) error {
	e.UpdateCurrentTables()

	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	if e.GzipReader == nil {
		e.GzipReader, err = gzip.NewReader(file)
		if err != nil {
			return err
		}
	} else {
		err = e.GzipReader.Reset(file)
		if err != nil {
			return err
		}
	}

	defer func() {
		err = e.GzipReader.Close()
		if err != nil {
			log.Printf("Error closing gzip reader body: %v.", err)
		}
		err = file.Close()
		if err != nil {
			log.Printf("Error closing %s: %v.", filename, err)
		}
	}()

	d := json.NewDecoder(e.GzipReader)
	d.UseNumber()
	for {
		var event MPEvent
		err := d.Decode(&event)
		if err == io.EOF {
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

// UpdateCurrentTables talks to scoop and updates the list of tables that have been created.
func (e *EventRouter) UpdateCurrentTables() {
	configs, err := e.ScoopClient.FetchAllSchemas()
	if err != nil {
		log.Printf("Error fetching schemas from scoop: %v", err)
		return
	}
	newTables := make([]string, len(configs))
	for idx, config := range configs {
		newTables[idx] = config.EventName
	}
	e.CurrentTables = newTables
}

// Route sends an event to its event aggregator, but only if the event does not have a table yet.
func (e *EventRouter) Route(eventName string, properties map[string]interface{}) {
	if e.EventCreated(eventName) {
		return
	}

	_, ok := e.Processors[eventName]
	if !ok {
		e.Processors[eventName] = e.ProcessorFactory(e.OutputDir)
	}
	e.Processors[eventName].Accept(properties)
}

// FlushRouters flushes event schema descriptions to the output directory, and also deletes ones for
// which a table has been created (can happen under race condition).
func (e *EventRouter) FlushRouters() {
	for event, processor := range e.Processors {
		processor.Flush(event)
		delete(e.Processors, event)
	}
	// removed tracked events here (at least limit the time of the race duration)
	e.UpdateCurrentTables()
	infos, err := ioutil.ReadDir(e.OutputDir)
	if err != nil {
		return
	}
	for _, info := range infos {
		if info.IsDir() {
			continue
		}
		if strings.HasSuffix(info.Name(), ".json") && e.EventCreated(strings.TrimSuffix(info.Name(), ".json")) {
			fname := path.Join(e.OutputDir, info.Name())
			err = os.Remove(fname)
			if err != nil {
				log.Printf("Error removing file %s: %v", fname, err)
			}
		}
	}
}

// EventCreated returns true if the event has a table in scoop.
func (e *EventRouter) EventCreated(eventName string) bool {
	for _, tables := range e.CurrentTables {
		if tables == eventName {
			return true
		}
	}
	return false
}

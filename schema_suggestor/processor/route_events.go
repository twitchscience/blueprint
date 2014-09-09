package processor

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/twitchscience/blueprint/scoopclient"
)

type EventRouter struct {
	CurrentTables    []string
	Processors       map[string]EventProcessor
	ProcessorFactory func(string) EventProcessor
	FlushTimer       <-chan time.Time
	ScoopClient      scoopclient.ScoopClient
	GzipReader       *gzip.Reader
	OutputDir        string
}

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
		e.GzipReader.Close()
		file.Close()
	}()

	d := json.NewDecoder(e.GzipReader)
	d.UseNumber()
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
		e.Processors[eventName] = e.ProcessorFactory(e.OutputDir)
	}
	e.Processors[eventName].Accept(properties)
}

func (e *EventRouter) FlushRouters() {
	for event, processor := range e.Processors {
		processor.Flush(event)
		delete(e.Processors, event)
	}
	// removed tracked events here (at least limit the time of the race duration)
	e.UpdateCurrentTables()
	filepath.Walk(e.OutputDir, func(path string, info os.FileInfo, err error) error {
		if path == e.OutputDir {
			return nil
		}
		if info.IsDir() {
			return filepath.SkipDir
		}
		eventNameIdx := strings.Index(info.Name(), ".")
		if eventNameIdx > 0 && info.Name()[eventNameIdx:len(info.Name())] == ".json" {
			if e.EventCreated(info.Name()[0:eventNameIdx]) {
				os.Remove(path)
			}
		}
		return nil
	})
}

func (e *EventRouter) EventCreated(eventName string) bool {
	for _, tables := range e.CurrentTables {
		if tables == eventName {
			return true
		}
	}
	return false
}

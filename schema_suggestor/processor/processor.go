// Package processor contains logic to process gzipped Mixpanel event data into SQL table schemas.
package processor

import (
	"log"
	"reflect"
)

var (
	// CriticalPercentage is the percentage of events that a property must be seen in in order to be considered part of the schema for an event.
	CriticalPercentage = 0.0

	// CriticalThreshold is the number of events of a specific event name that must occur for the event to be summarized.
	CriticalThreshold = 2
)

// EventProcessor processes events of a certain type and flushes metadata about the schema.
type EventProcessor interface {
	Accept(map[string]interface{})
	Flush(string)
}

// Outputter outputs a given event's property summary and number of rows.
type Outputter interface {
	Output(string, []PropertySummary, int) error
}

// NonTrackedEventProcessor takes in events
type NonTrackedEventProcessor struct {
	// Out outputs events to a directory.
	Out Outputter

	// Aggregator summarizes the properties for this event for the purposes of creating a SQL table.
	Aggregator *EventAggregator

	// In is the channel of event properties.
	In chan map[string]interface{}

	// F is a channel that receives the event name when we're done aggregating and want to compute the transformation.
	F chan string
}

// PropertySummary gives information about a field contained in an event.
type PropertySummary struct {
	// Name of the property.
	Name string

	// OccurrenceProbability is an estimate of how often the field appears when the event is sent.
	OccurrenceProbability float64

	// T is the Go type of the property.
	T reflect.Type

	// Len gives an approximate length of the values for this property if it is a string.
	Len int
}

// NewNonTrackedEventProcessor allocates a new NonTrackedEventProcessor.
func NewNonTrackedEventProcessor(outputDir string) EventProcessor {
	p := &NonTrackedEventProcessor{
		Out: NewOutputter(outputDir),
		In:  make(chan map[string]interface{}, 100),
		F:   make(chan string),
	}
	go p.Listen()
	return p
}

// Listen for events.
func (e *NonTrackedEventProcessor) Listen() {
	for {
		select {
		case p := <-e.In:
			if e.Aggregator == nil {
				e.Aggregator = NewEventAggregator(CriticalPercentage)
			}
			e.Aggregator.Aggregate(p)
		case eventName := <-e.F:
			// drain
			close(e.In)
			for p := range e.In {
				if e.Aggregator == nil {
					e.Aggregator = NewEventAggregator(CriticalPercentage)
				}
				e.Aggregator.Aggregate(p)
			}
			nRows, cols := e.Aggregator.Summarize()
			if nRows > CriticalThreshold {
				err := e.Out.Output(eventName, cols, nRows)
				if err != nil {
					log.Printf("Outputter error: %v\n", err)
				}
			}
			e.Aggregator = NewEventAggregator(CriticalPercentage)
			return
		}
	}
}

// Accept an event's properties.
func (e *NonTrackedEventProcessor) Accept(propertyBag map[string]interface{}) {
	e.In <- propertyBag
}

// Flush events received. Label the flush with a given name.
func (e *NonTrackedEventProcessor) Flush(eventName string) {
	e.F <- eventName
}

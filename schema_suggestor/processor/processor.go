package processor

import (
	"log"
	"reflect"
)

var (
	CRITICAL_PERCENTAGE = 15.0
	CRITICAL_THRESHOLD  = 10
)

type EventProcessor interface {
	Accept(map[string]interface{})
	Flush(string)
}

type Outputter interface {
	Output(string, []PropertySummary) error
}

type NonTrackedEventProcessor struct {
	Out        Outputter
	Aggregator *EventAggregator
}

type PropertySummary struct {
	Name string
	T    reflect.Type
	Len  int
}

func (e *NonTrackedEventProcessor) Accept(propertyBag map[string]interface{}) {
	// should check that flush ahasnt been called
	if e.Aggregator == nil {
		e.Aggregator = NewEventAggregator(CRITICAL_PERCENTAGE)
	}
	e.Aggregator.Aggregate(propertyBag)
}

func (e *NonTrackedEventProcessor) Flush(eventName string) {
	nRows, cols := e.Aggregator.Summarize()
	if nRows > CRITICAL_THRESHOLD {
		err := e.Out.Output(eventName, cols)
		if err != nil {
			log.Printf("Outputter error: %v\n", err)
		}
	}
	e.Aggregator = NewEventAggregator(CRITICAL_PERCENTAGE)
}

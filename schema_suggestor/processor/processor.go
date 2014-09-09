package processor

import (
	"log"
	"reflect"
)

var (
	CRITICAL_PERCENTAGE = 0.0
	CRITICAL_THRESHOLD  = 2
)

type EventProcessor interface {
	Accept(map[string]interface{})
	Flush(string)
}

type Outputter interface {
	Output(string, []PropertySummary, int) error
}

type NonTrackedEventProcessor struct {
	Out        Outputter
	Aggregator *EventAggregator
}

type PropertySummary struct {
	Name          string
	OccuranceRank float64
	T             reflect.Type
	Len           int
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
		err := e.Out.Output(eventName, cols, nRows)
		if err != nil {
			log.Printf("Outputter error: %v\n", err)
		}
	}
	e.Aggregator = NewEventAggregator(CRITICAL_PERCENTAGE)
}

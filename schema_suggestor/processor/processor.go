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
	In         chan map[string]interface{}
	F          chan string
}

type PropertySummary struct {
	Name                 string
	OccuranceProbability float64
	T                    reflect.Type
	Len                  int
}

func NewNonTrackedEventProcessor(outputDir string) EventProcessor {
	p := &NonTrackedEventProcessor{
		Out: NewOutputter(outputDir),
		In:  make(chan map[string]interface{}, 100),
		F:   make(chan string),
	}
	go p.Listen()
	return p
}

func (e *NonTrackedEventProcessor) Listen() {
	for {
		select {
		case p := <-e.In:
			if e.Aggregator == nil {
				e.Aggregator = NewEventAggregator(CRITICAL_PERCENTAGE)
			}
			e.Aggregator.Aggregate(p)
		case eventName := <-e.F:
			// drain
			close(e.In)
			for p := range e.In {
				if e.Aggregator == nil {
					e.Aggregator = NewEventAggregator(CRITICAL_PERCENTAGE)
				}
				e.Aggregator.Aggregate(p)
			}
			nRows, cols := e.Aggregator.Summarize()
			if nRows > CRITICAL_THRESHOLD {
				err := e.Out.Output(eventName, cols, nRows)
				if err != nil {
					log.Printf("Outputter error: %v\n", err)
				}
			}
			e.Aggregator = NewEventAggregator(CRITICAL_PERCENTAGE)
			return
		}
	}
}

func (e *NonTrackedEventProcessor) Accept(propertyBag map[string]interface{}) {
	e.In <- propertyBag
}

func (e *NonTrackedEventProcessor) Flush(eventName string) {
	e.F <- eventName
}

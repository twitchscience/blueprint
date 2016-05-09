package processor

import (
	"encoding/json"
	"reflect"
	"strconv"
)

const (
	stringType = "string"
)

// EventAggregator summarizes a set of events.
type EventAggregator struct {
	// CriticalPercent is the threshold percent of events that contain a given property, under which a property will be ommitted from the event summary.
	CriticalPercent float64

	// TotalRows is a count of events seen.
	TotalRows int

	// Columns stores information about each property contained within the event..
	Columns map[string]*TypeAggregator
}

// TypeAggregator counts values of potentially many different types.
type TypeAggregator struct {
	Total  int
	Counts map[string]*TypeCounter
}

// TypeCounter counts occurrences of a specific type.
type TypeCounter struct {
	Type         reflect.Type
	Count        int
	LenEstimator LengthEstimator
}

// NewEventAggregator allocates a new EventAggregator.
func NewEventAggregator(criticalPercentage float64) *EventAggregator {
	return &EventAggregator{
		CriticalPercent: criticalPercentage,
		Columns:         make(map[string]*TypeAggregator),
	}
}

// NewTypeAggregator allocates a new TypeAggregator.
func NewTypeAggregator() *TypeAggregator {
	return &TypeAggregator{
		Counts: make(map[string]*TypeCounter),
	}
}

// Aggregate JSON objects.
func (e *EventAggregator) Aggregate(properties map[string]interface{}) {
	for columnName, val := range properties {
		_, ok := e.Columns[columnName]
		if !ok {
			e.Columns[columnName] = NewTypeAggregator()
		}
		e.Columns[columnName].Aggregate(val)
	}
	e.TotalRows++
}

// Summarize returns a summary of the properties seen for this set of events, as well as a count of number of events seen.
// It prunes any property that didn't occur in over CriticalPercent of events.
func (e *EventAggregator) Summarize() (int, []PropertySummary) {
	var aggregatedTypes []PropertySummary
	for columnName, aggregator := range e.Columns {
		if e.ColumnShouldBePruned(aggregator) || aggregator.Total < 1 {
			continue
		}
		ps := aggregator.Summarize()
		ps.Name = columnName
		ps.OccurrenceProbability = float64(aggregator.Total) / float64(e.TotalRows)
		aggregatedTypes = append(aggregatedTypes, ps)
	}
	return e.TotalRows, aggregatedTypes
}

// ColumnShouldBePruned returns whether a property seen in set of events should be ignored.
func (e *EventAggregator) ColumnShouldBePruned(colAggregate *TypeAggregator) bool {
	return (float64(colAggregate.Total) / float64(e.TotalRows) * 100) < e.CriticalPercent
}

// Aggregate decoded JSON values. Converts json.Number to int or float.
func (t *TypeAggregator) Aggregate(val interface{}) {
	typ := reflect.TypeOf(val)
	if typ == nil {
		return
	}

	if typ.Name() == "Number" {
		// coerce into float or int
		typ = coerceJSONNumberToFloatOrInt(val.(json.Number))
	}

	_, ok := t.Counts[typ.Name()]
	if !ok {
		switch typ.Name() {
		case stringType:
			t.Counts[typ.Name()] = &TypeCounter{
				Type:         typ,
				LenEstimator: LengthEstimator{},
			}

		default:
			t.Counts[typ.Name()] = &TypeCounter{
				Type: typ,
			}
		}
	}
	t.Counts[typ.Name()].Aggregate(val)
	t.Total++
}

func coerceJSONNumberToFloatOrInt(n json.Number) reflect.Type {
	i, err := n.Int64()
	if err == nil && strconv.Itoa(int(i)) == n.String() {
		return reflect.TypeOf(int(i))
	}
	return reflect.TypeOf(123.2)
}

// Summarize returns the summary for the type that occurred the most.
func (t *TypeAggregator) Summarize() PropertySummary {
	max := &TypeCounter{
		Count: -1,
	}
	for _, counter := range t.Counts {
		if counter.Count > max.Count {
			max = counter
		}
	}
	return max.Summarize()
}

// Aggregate Go values of a single type. For strings, will store lengths of all strings for estimating 99th percentile.
func (c *TypeCounter) Aggregate(val interface{}) {
	if c.Type.Name() == stringType {
		s := val.(string)
		c.LenEstimator.Increment(len(s))
	}
	c.Count++
}

// Summarize values that have been aggregated.
func (c *TypeCounter) Summarize() PropertySummary {
	if c.Type.Name() == stringType {
		return PropertySummary{
			T:   c.Type,
			Len: c.LenEstimator.Estimate(),
		}
	}
	return PropertySummary{
		T: c.Type,
	}
}

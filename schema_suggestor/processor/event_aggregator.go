package processor

import (
	"encoding/json"
	"reflect"
	"strconv"
)

type EventAggregator struct {
	CriticalPercent float64
	TotalRows       int
	Columns         map[string]*TypeAggregator
}

type TypeAggregator struct {
	Total  int
	Counts map[string]*TypeCounter
}

type TypeCounter struct {
	Type         reflect.Type
	Count        int
	LenEstimator LengthEstimator
}

func NewEventAggregator(criticalPercentage float64) *EventAggregator {
	return &EventAggregator{
		CriticalPercent: criticalPercentage,
		Columns:         make(map[string]*TypeAggregator),
	}
}

func NewTypeAggregator() *TypeAggregator {
	return &TypeAggregator{
		Counts: make(map[string]*TypeCounter),
	}
}

func (e *EventAggregator) Aggregate(properties map[string]interface{}) {
	for columnName, val := range properties {
		if _, ok := e.Columns[columnName]; !ok {
			e.Columns[columnName] = NewTypeAggregator()
		}
		e.Columns[columnName].Aggregate(val)
	}
	e.TotalRows++
}

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

func (e *EventAggregator) ColumnShouldBePruned(colAggregate *TypeAggregator) bool {
	return (float64(colAggregate.Total) / float64(e.TotalRows) * 100) < e.CriticalPercent
}

func (t *TypeAggregator) Aggregate(val interface{}) {
	type_ := reflect.TypeOf(val)
	if type_ == nil {
		return
	}

	if type_.Name() == "Number" {
		// coerce into float or int
		type_ = coerceJsonNumberToFloatOrInt(val.(json.Number))
	}

	if _, ok := t.Counts[type_.Name()]; !ok {
		switch type_.Name() {
		case "string":
			t.Counts[type_.Name()] = &TypeCounter{
				Type:         type_,
				LenEstimator: LengthEstimator{},
			}

		default:
			t.Counts[type_.Name()] = &TypeCounter{
				Type: type_,
			}
		}
	}
	t.Counts[type_.Name()].Aggregate(val)
	t.Total++
}

func coerceJsonNumberToFloatOrInt(n json.Number) reflect.Type {
	i, err := n.Int64()
	if err == nil && strconv.Itoa(int(i)) == n.String() {
		return reflect.TypeOf(int(i))
	}
	return reflect.TypeOf(123.2)
}

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

func (c *TypeCounter) Aggregate(val interface{}) {
	if c.Type.Name() == "string" {
		s := val.(string)
		c.LenEstimator.Increment(len(s))
	}
	c.Count++
}

func (c *TypeCounter) Summarize() PropertySummary {
	if c.Type.Name() == "string" {
		return PropertySummary{
			T:   c.Type,
			Len: c.LenEstimator.Estimate(),
		}
	}
	return PropertySummary{
		T: c.Type,
	}
}

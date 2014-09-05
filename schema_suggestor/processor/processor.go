package processor

import (
	"fmt"
	"log"
	"reflect"
	"sort"
)

const CRITICAL_PERCENT = 85

type EventProcessor interface {
	Accept(map[string]interface{})
	Flush(string)
}

type Outputter interface {
	Output(string, []*PropertySummary) error
}

type NonTrackedEventProcessor struct {
	Out    Outputter
	Events []*PropertySummary
}

type PropertySummary struct {
	Name string
	T    reflect.Type
	Len  int
}

func (e *NonTrackedEventProcessor) Accept(propertyBag map[string]interface{}) {
	// should check that flush ahasnt been called
	for k, v := range propertyBag {
		e.Events = append(e.Events, newPropertySummary(k, v))
	}
}

func (e *NonTrackedEventProcessor) Flush(eventName string) {
	buckets := map[string][]*PropertySummary{}
	for _, s := range e.Events {
		if _, ok := buckets[s.Name]; !ok {
			buckets[s.Name] = []*PropertySummary{s}
		} else {
			buckets[s.Name] = append(buckets[s.Name], s)
		}
	}

	var aggregatedBuckets []*PropertySummary
	for k, v := range buckets {
		t, pct := assessType(v, len(e.Events))
		if pct < CRITICAL_PERCENT {
			continue
		}
		ps := &PropertySummary{
			Name: k,
			T:    t,
		}
		if t.Name() == "string" {
			ps.Len = p99(v)
		}
		aggregatedBuckets = append(aggregatedBuckets, ps)
	}
	err := e.Out.Output(eventName, aggregatedBuckets)
	if err != nil {
		log.Printf("Outputter error: %v\n", err)
	}
	e.Events = make([]*PropertySummary, 0)
}

func percentage(n []*PropertySummary, total int) string {
	return fmt.Sprintf("%.2f", float64(len(n))/float64(total)*100.0)
}

func assessType(n []*PropertySummary, total int) (reflect.Type, int) {
	typeCnt := map[reflect.Type]int{}
	for _, s := range n {
		typeCnt[s.T]++
	}
	t := n[0].T
	max := typeCnt[t]
	for k, v := range typeCnt {
		if v > max {
			t = k
			max = v
		} else if v == max && k != t {
			log.Println(t, "and", k.Name(), "occur as frequently as eachother")
		}
	}

	return t, int((float64(max) / float64(total)) * 100)
}

func p99(n []*PropertySummary) int {
	var strs []int
	for _, s := range n {
		if s.T.Name() == "string" {
			strs = append(strs, s.Len)
		}
	}
	sort.Ints(strs)
	idx := int((float64(len(strs)) / 100) * 99)
	return strs[idx]
}

func newPropertySummary(key string, val interface{}) *PropertySummary {
	t := reflect.TypeOf(val)
	var size int
	if t.Name() == "string" {
		size = len(val.(string))
	}
	return &PropertySummary{
		Name: key,
		T:    t,
		Len:  size,
	}
}

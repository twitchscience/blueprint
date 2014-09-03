package processor

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"reflect"
	"sort"
)

type objectSummary struct {
	name string
	t    reflect.Type
	len  int
}

func processData(in io.Reader) {
	var summaries []objectSummary
	for _, d := range readData(in) {
		for k, v := range d {
			summaries = append(summaries, *newObjectSummary(k, v))
		}
	}

	buckets := map[string][]objectSummary{}
	for _, s := range summaries {
		if _, ok := buckets[s.name]; !ok {
			buckets[s.name] = []objectSummary{s}
		} else {
			buckets[s.name] = append(buckets[s.name], s)
		}
	}

	propertyNames := []string{}
	for k, _ := range buckets {
		propertyNames = append(propertyNames, k)
	}
	sort.Strings(propertyNames)

	for _, k := range propertyNames {
		v := buckets[k]
		log.Println(k)
		log.Println("\toccurs", percentage(v, len(summaries)), "% of the time")
		t, pct := assessType(v, len(summaries))
		log.Println("\tis a", t, pct, "% of the time")
		if t == "string" {
			log.Println("\tand has a P99 length of", p99(v))
		}
	}
}

func percentage(n []objectSummary, total int) string {
	return fmt.Sprintf("%.2f", float64(len(n))/float64(total)*100.0)
}

func assessType(n []objectSummary, total int) (string, string) {
	typeCnt := map[reflect.Type]int{}
	for _, s := range n {
		typeCnt[s.t]++
	}
	t := n[0].t
	max := typeCnt[t]
	for k, v := range typeCnt {
		if v > max {
			t = k
			max = v
		} else if v == max && k != t {
			log.Println(t.Name(), "and", k.Name(), "occur as frequently as eachother")
		}
	}

	return t.Name(), fmt.Sprintf("%.2f", (float64(max)/float64(total))*100.0)
}

func p99(n []objectSummary) int {
	var strs []int
	for _, s := range n {
		if s.t.Name() == "string" {
			strs = append(strs, s.len)
		}
	}
	sort.Ints(strs)
	idx := int((float64(len(strs)) / 100) * 99)
	return strs[idx]
}

func newObjectSummary(key string, val interface{}) *objectSummary {
	t := reflect.TypeOf(val)
	var size int
	if t.Name() == "string" {
		size = len(val.(string))
	}
	return &objectSummary{
		name: key,
		t:    t,
		len:  size,
	}
}

func readData(in io.Reader) []map[string]interface{} {
	var data []map[string]interface{}
	dec := json.NewDecoder(in)
	for {
		var v map[string]interface{}
		if err := dec.Decode(&v); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		data = append(data, v)
	}
	return data
}

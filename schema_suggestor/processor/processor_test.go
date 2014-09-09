package processor

import (
	"bytes"
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

type TestOutputter struct {
	P []PropertySummary
}

func (t *TestOutputter) Output(e string, p []PropertySummary) error {
	t.P = p
	return nil
}

type ByName []PropertySummary

func (a ByName) Len() int           { return len(a) }
func (a ByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByName) Less(i, j int) bool { return a[i].Name < a[j].Name }

func TestNonTrackedEventProcessor(t *testing.T) {
	o := &TestOutputter{}
	e := NonTrackedEventProcessor{
		Out:        o,
		Aggregator: NewEventAggregator(15.0),
	}

	testEvent1 := map[string]interface{}{
		"col1": "12323",
		"col2": json.Number("123"),
		"col3": "1234525",
		"col4": json.Number("123.12"),
	}
	testEvent2 := map[string]interface{}{
		"col1": "123",
		"col2": json.Number("142"),
		"col4": json.Number("123.12"),
	}
	for i := 0; i < 10; i++ {
		e.Accept(testEvent2)
	}
	e.Accept(testEvent1)

	e.Flush("test")
	expected := []PropertySummary{
		PropertySummary{
			Name: "col1",
			T:    reflect.TypeOf("string"),
			Len:  4,
		},
		PropertySummary{
			Name: "col2",
			T:    reflect.TypeOf(12),
		},
		PropertySummary{
			Name: "col4",
			T:    reflect.TypeOf(12.1),
		},
	}
	sort.Sort(ByName(o.P))
	if !reflect.DeepEqual(o.P, expected) {
		t.Errorf("expected %+v but got %+v\n", expected, o.P)
		t.Fail()
	}
}

func TestScoopTransformer(t *testing.T) {
	input := []PropertySummary{
		PropertySummary{
			Name: "col1",
			T:    reflect.TypeOf("string"),
			Len:  4,
		},
		PropertySummary{
			Name: "col2",
			T:    reflect.TypeOf(12),
		},
	}

	out, err := ScoopTransformer("test", input)
	if err != nil {
		t.Fatal(err)
	}

	expected, err := json.Marshal(&scoop_protocol.Config{
		EventName: "test",
		Columns: []scoop_protocol.ColumnDefinition{
			scoop_protocol.ColumnDefinition{
				InboundName:           "col1",
				OutboundName:          "col1",
				Transformer:           "varchar",
				ColumnCreationOptions: "(4)",
			},
			scoop_protocol.ColumnDefinition{
				InboundName:  "col2",
				OutboundName: "col2",
				Transformer:  "bigint",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(expected, out) {
		t.Errorf("Expected %v to be %v\n", string(out), string(expected))
		t.Fail()
	}
}

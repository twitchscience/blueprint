package processor

import (
	"encoding/json"
	"io/ioutil"
	"strconv"
)

type outputter struct {
	transformer func(string, []PropertySummary, int) ([]byte, error)
	dumper      func(string, []byte) error
}

// FileDumper writes event data to a file within a directory.
type FileDumper struct {
	// TargetDir is the directory to write files to.
	TargetDir string
}

// AugmentedColumnDefinition adds some metadata to a property of an event.
type AugmentedColumnDefinition struct {
	// InboundName is the name of the property as sent to edge servers.
	InboundName string

	// OutboundName is the name of the property that will be stored in redshift.
	OutboundName string

	// Transformer is the SQL type for the column corresponding to this property.
	Transformer string

	// ColumnCreationOptions are additional options/parameters for the SQL type, e.g. for varchar, "(255)"
	ColumnCreationOptions string

	// OccurrenceProbability is how often this property appears in events.
	OccurrenceProbability float64
}

// AugmentedEventConfig gives the configuration for creating a table for a given event name.
type AugmentedEventConfig struct {

	// EventName is the name of the event.
	EventName string

	// Columns is the metadata required to create columns for each property of the event.
	Columns []AugmentedColumnDefinition

	// Occurred is the number of times the event occurred.
	Occurred int
}

// NewOutputter create an Outputter that writes event transformation configs to a directory.
func NewOutputter(targetDir string) Outputter {
	d := &FileDumper{
		TargetDir: targetDir,
	}
	return &outputter{
		transformer: ScoopTransformer,
		dumper:      d.Dumper,
	}
}

// Output transformation config for a particular event name.
func (o *outputter) Output(eventName string, properties []PropertySummary, nRows int) error {
	output, err := o.transformer(eventName, properties, nRows)
	if err != nil {
		return err
	}

	return o.dumper(eventName, output)
}

// Dumper writes a event data as a JSON file in the given target directory.
func (f *FileDumper) Dumper(event string, output []byte) error {
	return ioutil.WriteFile(f.TargetDir+"/"+event+".json", output, 0644)
}

// ScoopTransformer returns JSON corresponding to the configuration for converting events of a given event name to a SQL table.
func ScoopTransformer(eventName string, properties []PropertySummary, nRows int) ([]byte, error) {
	cols := make([]AugmentedColumnDefinition, len(properties))
	for idx, p := range properties {
		transformer, options := selectTransformerForProperty(p)
		cols[idx] = AugmentedColumnDefinition{
			InboundName:           p.Name,
			OutboundName:          p.Name,
			Transformer:           transformer,
			ColumnCreationOptions: options,
			OccurrenceProbability: p.OccurrenceProbability,
		}
	}

	return json.Marshal(&AugmentedEventConfig{
		EventName: eventName,
		Columns:   cols,
		Occurred:  nRows,
	})
}

func selectTransformerForProperty(property PropertySummary) (string, string) {
	switch property.T.Name() {
	case "bool":
		return "bool", ""
	case "int":
		return "bigint", ""
	case "string":
		return "varchar", "(" + strconv.Itoa(property.Len) + ")"
	case "float64":
		return "float", ""
	default:
		return "", ""
	}
}

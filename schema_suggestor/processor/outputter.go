package processor

import (
	"encoding/json"
	"io/ioutil"
	"strconv"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

type outputter struct {
	transformer func(string, []PropertySummary) ([]byte, error)
	dumper      func(string, []byte) error
}

type FileDumper struct {
	TargetDir string
}

func NewOutputter(targetDir string) Outputter {
	d := &FileDumper{
		TargetDir: targetDir,
	}
	return &outputter{
		transformer: ScoopTransformer,
		dumper:      d.Dumper,
	}
}

func (o *outputter) Output(eventName string, properties []PropertySummary) error {
	output, err := o.transformer(eventName, properties)
	if err != nil {
		return err
	}

	return o.dumper(eventName, output)
}

func (f *FileDumper) Dumper(event string, output []byte) error {
	// if file exists overwrite. Else do nothing
	return ioutil.WriteFile(f.TargetDir+"/"+event+".json", output, 0644)
}

func ScoopTransformer(eventName string, properties []PropertySummary) ([]byte, error) {
	cols := make([]scoop_protocol.ColumnDefinition, len(properties))
	for idx, p := range properties {
		transformer, options := selectTransformerForProperty(p)
		cols[idx] = scoop_protocol.ColumnDefinition{
			InboundName:           p.Name,
			OutboundName:          p.Name,
			Transformer:           transformer,
			ColumnCreationOptions: options,
		}
	}

	return json.Marshal(&scoop_protocol.Config{
		EventName: eventName,
		Columns:   cols,
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

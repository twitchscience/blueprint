package bpdb

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

func column(name, transformer, options, columns string) scoop_protocol.ColumnDefinition {
	return scoop_protocol.ColumnDefinition{
		InboundName:           name,
		OutboundName:          name,
		Transformer:           transformer,
		ColumnCreationOptions: options,
		SupportingColumns:     columns,
	}
}

func bigintColumn(name string) scoop_protocol.ColumnDefinition {
	return column(name, "bigint", "", "")
}

func varcharColumn(name string, length int, columns string) scoop_protocol.ColumnDefinition {
	return column(name, "varchar", fmt.Sprintf("(%d)", length), columns)
}

func TestApplyOperationAddColumns(t *testing.T) {
	base := AnnotatedSchema{
		EventName: "video_ad_request_error",
		Columns: []scoop_protocol.ColumnDefinition{
			varcharColumn("backend", 32, ""),
			varcharColumn("content_mode", 32, ""),
			varcharColumn("quality", 16, ""),
		},
	}
	ops := []scoop_protocol.Operation{
		scoop_protocol.NewAddOperation("minutes_logged", "minutes_logged", "bigint", "", ""),
		scoop_protocol.NewDeleteOperation("backend"),
		scoop_protocol.NewAddOperation("os", "os", "varchar", "(16)", ""),
		scoop_protocol.NewAddOperation("id", "id", "idVarchar", "(32)", "os"),
	}
	expected := AnnotatedSchema{
		EventName: "video_ad_request_error",
		Columns: []scoop_protocol.ColumnDefinition{
			varcharColumn("content_mode", 32, ""),
			varcharColumn("quality", 16, ""),
			bigintColumn("minutes_logged"),
			varcharColumn("os", 16, ""),
			column("id", "idVarchar", "(32)", "os"),
		},
	}

	if err := ApplyOperations(&base, ops); err != nil || !reflect.DeepEqual(expected, base) {
		t.Errorf("Results schema differs from expected:\n%v\nvs\n%v.", base, expected)
	}
}

func TestApplyOperationAddDupeColumns(t *testing.T) {
	base := AnnotatedSchema{
		EventName: "video_ad_request_error",
		Columns:   []scoop_protocol.ColumnDefinition{varcharColumn("backend", 32, "")},
	}
	ops := []scoop_protocol.Operation{
		scoop_protocol.NewAddOperation("minutes_logged", "minutes_logged", "bigint", "", ""),
		scoop_protocol.NewAddOperation("backend", "ip", "varchar", "(32)", ""),
	}
	if err := ApplyOperations(&base, ops); err == nil {
		t.Error("Expected error on adding existing row.")
	}
}

func TestApplyOperationDeleteNonExistentColumns(t *testing.T) {
	base := AnnotatedSchema{
		EventName: "video_ad_request_error",
		Columns:   []scoop_protocol.ColumnDefinition{varcharColumn("backend", 32, "")},
	}
	ops := []scoop_protocol.Operation{scoop_protocol.NewDeleteOperation("minutes_logged")}

	if err := ApplyOperations(&base, ops); err == nil {
		t.Error("Expected error on adding existing row.")
	}
}

package bpdb

import (
	"reflect"
	"testing"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

func TestApplyOperationAddColumns(t *testing.T) {
	base := scoop_protocol.Config{
		EventName: "video_ad_request_error",
		Columns: []scoop_protocol.ColumnDefinition{
			scoop_protocol.ColumnDefinition{InboundName: "backend", OutboundName: "backend", Transformer: "varchar", ColumnCreationOptions: "(32)"},
			scoop_protocol.ColumnDefinition{InboundName: "content_mode", OutboundName: "content_mode", Transformer: "varchar", ColumnCreationOptions: "(32)"},
			scoop_protocol.ColumnDefinition{InboundName: "quality", OutboundName: "quality", Transformer: "varchar", ColumnCreationOptions: "(16)"},
		},
	}
	ops := []Operation{
		Operation{"add", "minutes_logged", "minutes_logged", "bigint", ""},
		Operation{"delete", "backend", "backend", "varchar", "(32)"},
		Operation{"add", "os", "os", "varchar", "(16)"},
	}
	expected := scoop_protocol.Config{
		EventName: "video_ad_request_error",
		Columns: []scoop_protocol.ColumnDefinition{
			scoop_protocol.ColumnDefinition{InboundName: "content_mode", OutboundName: "content_mode", Transformer: "varchar", ColumnCreationOptions: "(32)"},
			scoop_protocol.ColumnDefinition{InboundName: "quality", OutboundName: "quality", Transformer: "varchar", ColumnCreationOptions: "(16)"},
			scoop_protocol.ColumnDefinition{InboundName: "minutes_logged", OutboundName: "minutes_logged", Transformer: "bigint", ColumnCreationOptions: ""},
			scoop_protocol.ColumnDefinition{InboundName: "os", OutboundName: "os", Transformer: "varchar", ColumnCreationOptions: "(16)"},
		},
	}
	err := ApplyOperations(&base, ops)
	if err != nil || !reflect.DeepEqual(expected, base) {
		t.Errorf("Results schema differs from expected:\n%v\nvs\n%v.", base, expected)
	}
}

func TestApplyOperationAddDupeColumns(t *testing.T) {
	base := scoop_protocol.Config{
		EventName: "video_ad_request_error",
		Columns: []scoop_protocol.ColumnDefinition{
			scoop_protocol.ColumnDefinition{InboundName: "backend", OutboundName: "backend", Transformer: "varchar", ColumnCreationOptions: "(32)"},
		},
	}
	ops := []Operation{
		Operation{"add", "minutes_logged", "minutes_logged", "bigint", ""},
		Operation{"add", "ip", "backend", "varchar", "(32)"}, // same outbound name as base col
	}
	err := ApplyOperations(&base, ops)
	if err == nil {
		t.Error("Expected error on adding existing row.")
	}
}

func TestApplyOperationDeleteNonExistentColumns(t *testing.T) {
	base := scoop_protocol.Config{
		EventName: "video_ad_request_error",
		Columns: []scoop_protocol.ColumnDefinition{
			scoop_protocol.ColumnDefinition{InboundName: "backend", OutboundName: "backend", Transformer: "varchar", ColumnCreationOptions: "(32)"},
		},
	}
	ops := []Operation{
		Operation{"delete", "minutes_logged", "minutes_logged", "bigint", ""}, // delete non-existent column
	}
	err := ApplyOperations(&base, ops)
	if err == nil {
		t.Error("Expected error on adding existing row.")
	}
}

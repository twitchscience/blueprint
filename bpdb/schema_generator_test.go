package bpdb

import (
	"reflect"
	"testing"
)

func TestApplyOperationAddColumns(t *testing.T) {
	base := Schema{
		EventName: "video_ad_request_error",
		Columns: []Column{
			Column{"backend", "backend", "varchar", "(32)"},
			Column{"content_mode", "content_mode", "varchar", "(32)"},
			Column{"quality", "quality", "varchar", "(16)"},
		},
	}
	ops := []Operation{
		Operation{"add", "minutes_logged", "minutes_logged", "bigint", ""},
		Operation{"add", "os", "os", "varchar", "(16)"},
	}
	expected := Schema{
		EventName: "video_ad_request_error",
		Columns: []Column{
			Column{"backend", "backend", "varchar", "(32)"},
			Column{"content_mode", "content_mode", "varchar", "(32)"},
			Column{"quality", "quality", "varchar", "(16)"},
			Column{"minutes_logged", "minutes_logged", "bigint", ""},
			Column{"os", "os", "varchar", "(16)"},
		},
	}
	err := base.ApplyOperations(ops)
	if err != nil || !reflect.DeepEqual(expected, base) {
		t.Errorf("Results schema differs from expected:\n%v\nvs\n%v.", base, expected)
	}
}

func TestApplyOperationAddDupeColumns(t *testing.T) {
	base := Schema{
		EventName: "video_ad_request_error",
		Columns: []Column{
			Column{"backend", "backend", "varchar", "(32)"},
		},
	}
	ops := []Operation{
		Operation{"add", "minutes_logged", "minutes_logged", "bigint", ""},
		Operation{"add", "ip", "backend", "varchar", "(32)"}, // same outbound name as base col
	}
	err := base.ApplyOperations(ops)
	if err == nil {
		t.Error("Expected error on adding existing row.")
	}
}

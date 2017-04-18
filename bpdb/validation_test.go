package bpdb

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

func TestPreValidateSchemaBadType(t *testing.T) {
	cfg := scoop_protocol.Config{
		EventName: "name",
		Columns: []scoop_protocol.ColumnDefinition{
			{
				InboundName:           "this",
				OutboundName:          "that",
				Transformer:           "invalidtype",
				ColumnCreationOptions: "",
				SupportingColumns:     "",
			},
		},
		Version: 0,
	}
	require.NotNil(t, preValidateSchema(&cfg), "Expected error on invalid type.")
}

func TestPreValidateSchemaOkay(t *testing.T) {
	cfg := scoop_protocol.Config{
		EventName: "name",
		Columns: []scoop_protocol.ColumnDefinition{
			{
				InboundName:           "this",
				OutboundName:          "that",
				Transformer:           "bigint",
				ColumnCreationOptions: "",
				SupportingColumns:     "",
			},
			{
				InboundName:           "foo",
				OutboundName:          "bar",
				Transformer:           "bigint",
				ColumnCreationOptions: "",
				SupportingColumns:     "",
			},
		},
		Version: 0,
	}
	require.Nil(t, preValidateSchema(&cfg), "Expected no error on valid schema.")
}

func TestPreValidateSchemaManyColumns(t *testing.T) {
	columns := []scoop_protocol.ColumnDefinition{}
	for i := 0; i < 301; i++ {
		col := scoop_protocol.ColumnDefinition{
			InboundName:           "this",
			OutboundName:          fmt.Sprintf("that%d", i),
			Transformer:           "bigint",
			ColumnCreationOptions: "",
			SupportingColumns:     "",
		}
		columns = append(columns, col)
	}
	cfg := scoop_protocol.Config{
		EventName: "name",
		Columns:   columns,
		Version:   0,
	}
	require.NotNil(t, preValidateSchema(&cfg), "Expected error on too many columns.")
}

func TestPreValidateSchemaColumnCollision(t *testing.T) {
	cfg := scoop_protocol.Config{
		EventName: "name",
		Columns: []scoop_protocol.ColumnDefinition{
			{
				InboundName:           "this",
				OutboundName:          "that",
				Transformer:           "invalidtype",
				ColumnCreationOptions: "",
				SupportingColumns:     "",
			},
			{
				InboundName:           "foo",
				OutboundName:          "that",
				Transformer:           "invalidtype",
				ColumnCreationOptions: "",
				SupportingColumns:     "",
			},
		},
		Version: 0,
	}
	require.NotNil(t, preValidateSchema(&cfg), "Expected error on duplicate column.")
}

func TestValidateIdentifierTooLong(t *testing.T) {
	err := validateIdentifier("1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890")
	require.NotNil(t, err, "Expected error on too long identifier.")
}

func TestValidateIdentifierBadCharacters(t *testing.T) {
	err := validateIdentifier("minute/watched")
	require.NotNil(t, err, "Expected error on bad characters in identifier.")
}

func TestValidateIdentifierValid(t *testing.T) {
	err := validateIdentifier("minute-watched")
	require.Nil(t, err, "Expected no error on valid identifier.")
}

func TestPreValidateUpdateEmpty(t *testing.T) {
	req := core.ClientUpdateSchemaRequest{
		EventName: "test",
		Additions: []core.Column{},
		Deletes:   []string{},
		Renames:   core.Renames{},
	}
	schema := AnnotatedSchema{
		EventName: "test",
		Columns:   []scoop_protocol.ColumnDefinition{},
	}
	requestErr := preValidateUpdate(&req, &schema)
	require.Equal(t, requestErr, "")
}

func TestPreValidateUpdateDropped(t *testing.T) {
	req := core.ClientUpdateSchemaRequest{
		EventName: "test",
		Additions: []core.Column{},
		Deletes:   []string{},
		Renames:   core.Renames{},
	}
	schema := AnnotatedSchema{
		EventName: "test",
		Columns:   []scoop_protocol.ColumnDefinition{},
		Dropped:   true,
	}
	requestErr := preValidateUpdate(&req, &schema)
	require.Equal(t, requestErr, "Attempted to modify drop-requested/dropped schema")
}

func TestPreValidateUpdateDeleteErrors(t *testing.T) {
	req := core.ClientUpdateSchemaRequest{
		EventName: "test",
		Additions: []core.Column{},
		Deletes:   []string{"x"},
		Renames:   core.Renames{},
	}
	schema := AnnotatedSchema{
		EventName: "test",
		Columns:   []scoop_protocol.ColumnDefinition{},
	}
	requestErr := preValidateUpdate(&req, &schema)
	require.Equal(t, requestErr, "Attempting to delete column that doesn't exist: x")

	schema.Columns = []scoop_protocol.ColumnDefinition{
		{OutboundName: "x", ColumnCreationOptions: "distkey"},
	}
	requestErr = preValidateUpdate(&req, &schema)
	require.Equal(t, requestErr, "Column is a key and cannot be dropped: x")
}

func TestPreValidateUpdateAddDelete(t *testing.T) {
	req := core.ClientUpdateSchemaRequest{
		EventName: "test",
		Additions: []core.Column{{OutboundName: "x", Transformer: "bool"}},
		Deletes:   []string{"x"},
		Renames:   core.Renames{},
	}
	schema := AnnotatedSchema{
		EventName: "test",
		Columns:   []scoop_protocol.ColumnDefinition{{OutboundName: "x"}},
	}
	requestErr := preValidateUpdate(&req, &schema)
	require.Equal(t, requestErr, "")
}

func TestPreValidateUpdateAddErrors(t *testing.T) {
	require := require.New(t)
	req := core.ClientUpdateSchemaRequest{
		EventName: "test",
		Additions: []core.Column{{OutboundName: ""}},
		Deletes:   []string{},
		Renames:   core.Renames{},
	}
	schema := AnnotatedSchema{
		EventName: "test",
		Columns:   []scoop_protocol.ColumnDefinition{},
	}
	requestErr := preValidateUpdate(&req, &schema)
	require.Equal(requestErr[:28], "Column outbound name invalid")

	req.Additions[0].OutboundName = "x"
	requestErr = preValidateUpdate(&req, &schema)
	require.Equal(requestErr[:26], "Column transformer invalid")

	req.Additions[0].Transformer = "bool"
	requestErr = preValidateUpdate(&req, &schema)
	require.Equal(requestErr, "")

	req.Additions = append(req.Additions, core.Column{OutboundName: "x", Transformer: "bool"})
	requestErr = preValidateUpdate(&req, &schema)
	require.Equal(requestErr, "Attempting to add duplicate column: x")

	req.Additions = req.Additions[:1]
	schema.Columns = []scoop_protocol.ColumnDefinition{{OutboundName: "x"}}
	requestErr = preValidateUpdate(&req, &schema)
	require.Equal(requestErr, "Attempting to add duplicate column: x")
}

func TestPreValidateUpdateRenameErrors(t *testing.T) {
	require := require.New(t)
	req := core.ClientUpdateSchemaRequest{
		EventName: "test",
		Additions: []core.Column{},
		Deletes:   []string{},
		Renames:   core.Renames{"x": ""},
	}
	schema := AnnotatedSchema{
		EventName: "test",
		Columns:   []scoop_protocol.ColumnDefinition{{OutboundName: "x"}},
	}
	requestErr := preValidateUpdate(&req, &schema)
	require.Equal(requestErr[:30], "New name for column is invalid")

	req.Renames["x"] = "y"
	requestErr = preValidateUpdate(&req, &schema)
	require.Equal(requestErr, "")

	req.Renames["a"] = "b"
	requestErr = preValidateUpdate(&req, &schema)
	require.Equal(requestErr, "Attempting to rename column that doesn't exist: a")

	schema.Columns = append(schema.Columns, scoop_protocol.ColumnDefinition{OutboundName: "y"})
	req.Renames = core.Renames{"x": "z", "y": "x"}
	requestErr = preValidateUpdate(&req, &schema)
	require.Equal(requestErr[:33], "Cannot rename from or to a column")

	req.Renames = core.Renames{"y": "x"}
	requestErr = preValidateUpdate(&req, &schema)
	require.Equal(requestErr, "Attempting to rename to duplicate column: x")
}

func TestValidateKinesisConfigInvalidStreamName(t *testing.T) {
	require := require.New(t)
	var config KinesisWriterConfig
	err := json.Unmarshal([]byte(`
{
	"StreamName": "spade-downstream-prod-test",
	"StreamRole": "arn:aws:iam::123:role/spade-downstream-prod-test",
	"StreamType": "firehose",
	"Compress": false,
	"Events": {
		"minute-watched": {
			"Fields": [
				"time"
			]
		}
	},
	"BufferSize": 1024,
	"MaxAttemptsPerRecord": 10,
	"RetryDelay": "1s",
	"Globber": {
		"MaxSize": 990000,
		"MaxAge": "1s",
		"BufferLength": 1024
	},
	"Batcher": {
		"MaxSize": 990000,
		"MaxEntries": 500,
		"MaxAge": "1s",
		"BufferLength": 1024
	}
}
	`), &config)
	require.Nil(err, "Could not marshal JSON")
	req := AnnotatedKinesisConfig{
		StreamName:  "spade-downstream-prod-test",
		StreamType:  "firehose",
		SpadeConfig: config,
	}

	err = validateKinesisConfig(&req)
	require.Nil(err, "Base valid name test failed")

	req.StreamName = "a-name_with_va1id-symb0ls"
	req.SpadeConfig.StreamName = "a-name_with_va1id-symb0ls"
	err = validateKinesisConfig(&req)
	require.Nil(err, "Valid name with numbers and symbols failed")

	req.StreamName = "a bad name!"
	req.SpadeConfig.StreamName = "a bad name!"
	err = validateKinesisConfig(&req)
	require.NotNil(err, "Invalid name with bad characters did not fail")

	req.StreamName = "test1!"
	req.SpadeConfig.StreamName = "test2"
	err = validateKinesisConfig(&req)
	require.NotNil(err, "Stream name mismatch did not fail")
}

func TestValidateKinesisConfigInvalidStreamType(t *testing.T) {
	require := require.New(t)
	var config KinesisWriterConfig
	err := json.Unmarshal([]byte(`
{
	"StreamName": "spade-downstream-prod-test",
	"StreamRole": "arn:aws:iam::123:role/spade-downstream-prod-test",
	"StreamType": "firehose",
	"Compress": false,
	"Events": {
		"minute-watched": {
			"Fields": [
				"time"
			]
		}
	},
	"BufferSize": 1024,
	"MaxAttemptsPerRecord": 10,
	"RetryDelay": "1s",
	"Globber": {
		"MaxSize": 990000,
		"MaxAge": "1s",
		"BufferLength": 1024
	},
	"Batcher": {
		"MaxSize": 990000,
		"MaxEntries": 500,
		"MaxAge": "1s",
		"BufferLength": 1024
	}
}
	`), &config)
	require.Nil(err, "Could not marshal JSON")
	req := AnnotatedKinesisConfig{
		StreamName:  "spade-downstream-prod-test",
		StreamType:  "firehose",
		SpadeConfig: config,
	}

	err = validateKinesisConfig(&req)
	require.Nil(err, "Base firehose test invalid")

	req.StreamType = "badtype"
	err = validateKinesisConfig(&req)
	require.NotNil(err, "Bad type did not fail")

	req.StreamType = "stream"
	req.SpadeConfig.StreamType = "stream"
	err = validateKinesisConfig(&req)
	require.Nil(err, "Valid stream type deemed invalid")

	req.SpadeConfig.StreamType = "firehose"
	err = validateKinesisConfig(&req)
	require.NotNil(err, "Type mismatch did not fail")
}

package bpdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
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
			},
			{
				InboundName:           "foo",
				OutboundName:          "bar",
				Transformer:           "bigint",
				ColumnCreationOptions: "",
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
			},
			{
				InboundName:           "foo",
				OutboundName:          "that",
				Transformer:           "invalidtype",
				ColumnCreationOptions: "",
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

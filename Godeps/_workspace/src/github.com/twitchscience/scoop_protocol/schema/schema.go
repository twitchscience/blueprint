package schema

import "github.com/twitchscience/scoop_protocol/scoop_protocol"

type UpdateSchemaRequest struct {
	EventName string `json:"-"`
	Columns   []scoop_protocol.ColumnDefinition
}

func (u *UpdateSchemaRequest) ConvertToRedshiftUpdate() *scoop_protocol.Config {
	return &scoop_protocol.Config{
		EventName: u.EventName,
		Columns:   u.Columns,
	}
}

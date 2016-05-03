package bpdb

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // To register "postgres" with database/sql
	"github.com/twitchscience/blueprint/core"
)

var (
	allSchemasQuery = `
SELECT event, action, inbound, outbound, column_type, column_options, version, ordering
FROM operation
ORDER BY version ASC, ordering ASC
`
	addColumnQuery = `INSERT INTO operation
(event, action, inbound, outbound, column_type, column_options, version, ordering)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
`
	nextVersionQuery = `SELECT max(version) + 1
FROM operation
WHERE event = $1
GROUP BY event`
)

type PGConfig struct {
	DatabaseURL    string
	MaxConnections int
}

type postgresBackend struct {
	db *sql.DB
}

type operationRow struct {
	event          string
	action         string
	inbound        string
	outbound       string
	column_type    string
	column_options string
	version        int
	ordering       int
}

func NewPostgresBackend(cfg *PGConfig) (Bpdb, error) {
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("Got err %v while connecting to db.", err)
	}
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("Got err %v trying to ping the db.", err)
	}
	db.SetMaxOpenConns(cfg.MaxConnections)
	b := &postgresBackend{db: db}
	return b, nil
}

func (p *postgresBackend) UpdateSchema(req *core.ClientUpdateSchemaRequest) error {
	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("Error beginning transaction for schema update: %v.", err)
	}

	row := tx.QueryRow(nextVersionQuery, req.EventName)
	var newVersion int
	err = row.Scan(&newVersion)
	if err != nil {
		return fmt.Errorf("Error parsing response for version number for %s: %v.", req.EventName, err)
	}

	for i, col := range req.Columns {
		_, err = tx.Exec(addColumnQuery,
			req.EventName,
			"add",
			col.InboundName,
			col.OutboundName,
			col.Transformer,
			col.Length,
			newVersion,
			i,
		)
		if err != nil {
			rollErr := tx.Rollback()
			if rollErr != nil {
				return fmt.Errorf("Error rolling back commit: %v.", rollErr)
			}
			return fmt.Errorf("Error INSERTing row for new column on %s: %v", req.EventName, err)
		}
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("Error commiting schema update for %s: %v", req.EventName, err)
	}
	return nil
}

func (p *postgresBackend) AllSchemas() ([]Schema, error) {
	rows, err := p.db.Query(allSchemasQuery)
	if err != nil {
		return nil, fmt.Errorf("Error querying for all schemas: %v.", err)
	}
	ops := []operationRow{}
	for rows.Next() {
		var op operationRow
		err = rows.Scan(&op.event, &op.action, &op.inbound, &op.outbound, &op.column_type, &op.column_options, &op.version, &op.ordering)
		if err != nil {
			return nil, fmt.Errorf("Error parsing operation row: %v.", err)
		}

		ops = append(ops, op)
	}
	return generateSchemas(ops)
}

// generateSchemas creates schemas from a list of operations
// by applying the operations in the order they appear in the array
func generateSchemas(ops []operationRow) ([]Schema, error) {
	schemas := make(map[string]*Schema)
	for _, op := range ops {
		_, exists := schemas[op.event]
		if !exists {
			schemas[op.event] = &Schema{EventName: op.event}
		}
		schemas[op.event].ApplyOperation(Operation{
			action:         op.action,
			inbound:        op.inbound,
			outbound:       op.outbound,
			column_type:    op.column_type,
			column_options: op.column_options,
		})
	}
	ret := make([]Schema, len(schemas))

	i := 0
	for _, val := range schemas {
		ret[i] = *val
		i++
	}
	return ret, nil
}

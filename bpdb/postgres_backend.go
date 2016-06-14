package bpdb

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // To register "postgres" with database/sql
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

var (
	schemaQuery = `
SELECT event, action, inbound, outbound, column_type, column_options, version, ordering
FROM operation
WHERE event = $1
ORDER BY version ASC, ordering ASC
`
	allSchemasQuery = `
SELECT event, action, inbound, outbound, column_type, column_options, version, ordering
FROM operation
ORDER BY version ASC, ordering ASC
`
	migrationQuery = `
SELECT action, inbound, outbound, column_type, column_options
FROM operation
WHERE version = $1
AND event = $2
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

type postgresBackend struct {
	db *sql.DB
}

type operationRow struct {
	event         string
	action        string
	inbound       string
	outbound      string
	columnType    string
	columnOptions string
	version       int
	ordering      int
}

// NewPostgresBackend creates a postgres bpdb backend to interface with
// the schema store
func NewPostgresBackend(dbConnection string) (Bpdb, error) {
	db, err := sql.Open("postgres", dbConnection)
	if err != nil {
		return nil, fmt.Errorf("Got err %v while connecting to db.", err)
	}
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("Got err %v trying to ping the db.", err)
	}
	b := &postgresBackend{db: db}
	return b, nil
}

func (p *postgresBackend) Migration(table string, to int) ([]*scoop_protocol.Operation, error) {
	rows, err := p.db.Query(migrationQuery, to, table)
	if err != nil {
		return nil, fmt.Errorf("Error querying for migration (%s) to v%v: %v.", table, to, err)
	}
	ops := []*scoop_protocol.Operation{}
	for rows.Next() {
		var op scoop_protocol.Operation
		err := rows.Scan(&op.Action, &op.Inbound, &op.Outbound, &op.ColumnType, &op.ColumnOptions)
		if err != nil {
			return nil, fmt.Errorf("Error parsing operation row: %v.", err)
		}

		ops = append(ops, &op)
	}
	return ops, nil
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

func (p *postgresBackend) CreateSchema(req *scoop_protocol.Config) error {
	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("Error beginning transaction for schema creation: %v.", err)
	}

	for i, col := range req.Columns {
		_, err = tx.Exec(addColumnQuery,
			req.EventName,
			"add",
			col.InboundName,
			col.OutboundName,
			col.Transformer,
			col.ColumnCreationOptions,
			0,
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
		return fmt.Errorf("Error commiting schema creation for %s: %v", req.EventName, err)
	}
	return nil
}

// scanOperationRows scans the rows into operationRow objects
func scanOperationRows(rows *sql.Rows) ([]operationRow, error) {
	ops := []operationRow{}
	for rows.Next() {
		var op operationRow
		err := rows.Scan(&op.event, &op.action, &op.inbound, &op.outbound, &op.columnType, &op.columnOptions, &op.version, &op.ordering)
		if err != nil {
			return nil, fmt.Errorf("Error parsing operation row: %v.", err)
		}

		ops = append(ops, op)
	}
	return ops, nil
}

// Schema returns the current schema for the table `name`
func (p *postgresBackend) Schema(name string) (*scoop_protocol.Config, error) {
	rows, err := p.db.Query(schemaQuery, name)
	if err != nil {
		return nil, fmt.Errorf("Error querying for schema %s: %v.", name, err)
	}
	ops, err := scanOperationRows(rows)
	if err != nil {
		return nil, err
	}

	schemas, err := generateSchemas(ops)
	if err != nil {
		return nil, err
	}
	if len(schemas) > 1 {
		return nil, fmt.Errorf("Expected only one schema, received %v.", len(schemas))
	}
	if len(schemas) == 0 {
		return nil, fmt.Errorf("Unable to find schema: %v", name)
	}
	return &schemas[0], nil
}

// Schema returns all of the current schemas
func (p *postgresBackend) AllSchemas() ([]scoop_protocol.Config, error) {
	rows, err := p.db.Query(allSchemasQuery)
	if err != nil {
		return nil, fmt.Errorf("Error querying for all schemas: %v.", err)
	}
	ops, err := scanOperationRows(rows)
	if err != nil {
		return nil, err
	}
	return generateSchemas(ops)
}

// max returns the max of the two arguments
func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

// generateSchemas creates schemas from a list of operations
// by applying the operations in the order they appear in the array
func generateSchemas(ops []operationRow) ([]scoop_protocol.Config, error) {
	schemas := make(map[string]*scoop_protocol.Config)
	for _, op := range ops {
		_, exists := schemas[op.event]
		if !exists {
			schemas[op.event] = &scoop_protocol.Config{EventName: op.event}
		}
		err := ApplyOperation(schemas[op.event], Operation{
			action:        op.action,
			inbound:       op.inbound,
			outbound:      op.outbound,
			columnType:    op.columnType,
			columnOptions: op.columnOptions,
		})
		if err != nil {
			return []scoop_protocol.Config{}, fmt.Errorf("Error applying operation to schema: %v", err)
		}
		schemas[op.event].Version = max(schemas[op.event].Version, op.version)
	}
	ret := make([]scoop_protocol.Config, len(schemas))

	i := 0
	for _, val := range schemas {
		ret[i] = *val
		i++
	}
	return ret, nil
}

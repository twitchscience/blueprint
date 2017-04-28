package bpdb

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"encoding/json"

	_ "github.com/lib/pq" // to include the 'postgres' driver
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

var (
	schemaQuery = `
SELECT event, action, name, version, ordering, action_metadata, ts, user_name
FROM operation
WHERE event = $1
ORDER BY version ASC, ordering ASC
`
	allSchemasQuery = `
SELECT event, action, name,  version, ordering, action_metadata, ts, user_name
FROM operation
ORDER BY version ASC, ordering ASC
`
	migrationQuery = `
SELECT action, name, action_metadata
FROM operation
WHERE version = $1
AND event = $2
ORDER BY ordering ASC
`
	insertOperationsQuery = `INSERT INTO operation
(event, action, name, version, ordering, action_metadata, user_name)
VALUES ($1, $2, $3, $4, $5, $6, $7)
`

	nextVersionQuery = `SELECT max(version) + 1
FROM operation
WHERE event = $1
GROUP BY event`
)

type schemaBackend struct {
	db *sql.DB
}

type operationRow struct {
	event          string
	action         string
	name           string
	actionMetadata map[string]string
	version        int
	ordering       int
	ts             time.Time
	userName       string
}

// NewSchemaBackend creates a postgres bpdb backend to interface with
// the kinesis configuration store
func NewSchemaBackend(db *sql.DB) BpSchemaBackend {
	return &schemaBackend{db: db}
}

// Migration returns the operations necessary to migration `table` from version `to -1` to version `to`
func (p *schemaBackend) Migration(table string, to int) ([]*scoop_protocol.Operation, error) {
	rows, err := p.db.Query(migrationQuery, to, table)
	if err != nil {
		return nil, fmt.Errorf("querying for migration (%s) to v%v: %v", table, to, err)
	}
	ops := []*scoop_protocol.Operation{}
	defer func() {
		err := rows.Close()
		if err != nil {
			logger.WithError(err).Error("closing rows in postgres backend Migration")
		}
	}()
	for rows.Next() {
		var op scoop_protocol.Operation
		var b []byte
		var s string
		err := rows.Scan(&s, &op.Name, &b)
		if err != nil {
			return nil, fmt.Errorf("parsing row into Operation: %v", err)
		}

		op.Action = scoop_protocol.Action(s)
		err = json.Unmarshal(b, &op.ActionMetadata)
		if err != nil {
			return nil, fmt.Errorf("unmarshalling action_metadata: %v", err)
		}
		ops = append(ops, &op)
	}
	return ops, nil
}

//execFnInTransaction takes a closure function of a request and runs it on the db in a transaction
func (p *schemaBackend) execFnInTransaction(work func(*sql.Tx) error) error {
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	err = work(tx)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return fmt.Errorf("could not rollback successfully after error (%v), reason: %v", err, rollbackErr)
		}
		return err
	}
	return tx.Commit()
}

// returns error but does not rollback on error. Does not commit.
func insertOperations(tx *sql.Tx, ops []scoop_protocol.Operation, version int, eventName, user string) error {
	for i, op := range ops {
		var b []byte
		b, err := json.Marshal(op.ActionMetadata)
		if err != nil {
			return fmt.Errorf("marshalling %s column metadata json: %v", op.Action, err)
		}
		_, err = tx.Exec(insertOperationsQuery,
			eventName,
			string(op.Action),
			op.Name,
			version,
			i, // ordering
			b, // action_metadata
			user,
		)
		if err != nil {
			rollErr := tx.Rollback()
			if rollErr != nil {
				return fmt.Errorf("rolling back commit: %v", rollErr)
			}
			return fmt.Errorf("INSERTing operation row on %s: %v", eventName, err)
		}
	}
	return nil
}

// CreateSchema validates that the creation operation is valid and if so, stores
// the schema as 'add' operations in bpdb
func (p *schemaBackend) CreateSchema(req *scoop_protocol.Config, user string) *core.WebError {
	exists, err := p.SchemaExists(req.EventName)
	if err != nil {
		return core.NewServerWebErrorf("checking for schema existence: %v", err)
	}
	if exists {
		return core.NewUserWebErrorf("Table already exists")
	}
	err = preValidateSchema(req)
	if err != nil {
		return core.NewUserWebError(err)
	}

	ops := schemaCreateRequestToOps(req)
	return core.NewServerWebError(p.execFnInTransaction(func(tx *sql.Tx) error {
		row := tx.QueryRow(nextVersionQuery, req.EventName)
		var newVersion int
		err = row.Scan(&newVersion)
		switch {
		case err == sql.ErrNoRows:
			newVersion = 0
		case err != nil:
			return fmt.Errorf("parsing response for version number for %s: %v", req.EventName, err)
		}
		return insertOperations(tx, ops, newVersion, req.EventName, user)
	}))
}

// UpdateSchema validates that the update operation is valid and if so, stores
// the operations for this migration to the schema as operations in bpdb. It
// applies the operations in order of delete, add, then renames.
func (p *schemaBackend) UpdateSchema(req *core.ClientUpdateSchemaRequest, user string) *core.WebError {
	schema, err := p.Schema(req.EventName)
	if err != nil {
		return core.NewServerWebErrorf("error getting schema to validate schema update: %v", err)
	}
	if schema == nil {
		return core.NewUserWebError(errors.New("Unknown schema"))
	}
	requestErr := preValidateUpdate(req, schema)
	if requestErr != "" {
		return core.NewUserWebError(errors.New(requestErr))
	}
	ops := schemaUpdateRequestToOps(req)
	err = ApplyOperations(schema, ops)
	if err != nil {
		return core.NewServerWebErrorf("error applying update operations: %v", err)
	}

	return core.NewServerWebError(p.execFnInTransaction(func(tx *sql.Tx) error {
		row := tx.QueryRow(nextVersionQuery, req.EventName)
		var newVersion int
		err := row.Scan(&newVersion)
		if err != nil {
			return fmt.Errorf("parsing response for version number for %s: %v", req.EventName, err)
		}
		return insertOperations(tx, ops, newVersion, req.EventName, user)
	}))
}

// DropSchema drops or requests a drop for a schema, depending on whether it exists according to ingester.
func (p *schemaBackend) DropSchema(schema *AnnotatedSchema, reason string, exists bool, user string) error {
	return p.execFnInTransaction(func(tx *sql.Tx) error {
		var newVersion int
		row := tx.QueryRow(nextVersionQuery, schema.EventName)
		err := row.Scan(&newVersion)
		if err != nil {
			return fmt.Errorf("parsing response for version number for %s: %v", schema.EventName, err)
		}
		var op scoop_protocol.Operation
		if exists {
			op = scoop_protocol.NewRequestDropEventOperation(reason)
		} else {
			op = scoop_protocol.NewDropEventOperation(reason)
		}
		return insertOperations(tx, []scoop_protocol.Operation{op}, newVersion, schema.EventName, user)
	})
}

// SchemaExists checks if a schema name exists in blueprint already
func (p *schemaBackend) SchemaExists(eventName string) (bool, error) {
	schema, err := p.Schema(eventName)
	if err != nil {
		return false, fmt.Errorf("querying existence of schema  %s: %v", eventName, err)
	}
	return schema != nil, nil
}

// scanOperationRows scans the rows into operationRow objects
func scanOperationRows(rows *sql.Rows) ([]operationRow, error) {
	ops := []operationRow{}
	defer func() {
		err := rows.Close()
		if err != nil {
			logger.WithError(err).Error("closing rows in postgres backend scanOperationRows")
		}
	}()
	for rows.Next() {
		var op operationRow
		var b []byte
		err := rows.Scan(&op.event, &op.action, &op.name, &op.version, &op.ordering, &b, &op.ts, &op.userName)
		if err != nil {
			return nil, fmt.Errorf("parsing operation row: %v", err)
		}
		err = json.Unmarshal(b, &op.actionMetadata)
		if err != nil {
			return nil, fmt.Errorf("unmarshalling action_metadata: %v", err)
		}
		ops = append(ops, op)
	}
	return ops, nil
}

// Schema returns the current schema for the table `name`
func (p *schemaBackend) Schema(name string) (*AnnotatedSchema, error) {
	rows, err := p.db.Query(schemaQuery, name)
	if err != nil {
		return nil, fmt.Errorf("querying for schema %s: %v", name, err)
	}
	ops, err := scanOperationRows(rows)
	if err != nil {
		return nil, err
	}

	schemas, err := generateSchemas(ops)
	if err != nil {
		return nil, fmt.Errorf("generating schemas from operations: %v", err)
	}
	if len(schemas) > 1 {
		return nil, fmt.Errorf("expected only one schema, received %v", len(schemas))
	}
	if len(schemas) == 0 {
		return nil, nil
	}
	return &schemas[0], nil
}

// Schema returns all of the current schemas
func (p *schemaBackend) AllSchemas() ([]AnnotatedSchema, error) {
	rows, err := p.db.Query(allSchemasQuery)
	if err != nil {
		return nil, fmt.Errorf("querying for all schemas: %v", err)
	}
	ops, err := scanOperationRows(rows)
	if err != nil {
		return nil, err
	}
	return generateSchemas(ops)
}

// generateSchemas creates schemas from a list of operations
// by applying the operations in the order they appear in the array
func generateSchemas(ops []operationRow) ([]AnnotatedSchema, error) {
	schemas := make(map[string]*AnnotatedSchema)
	for _, op := range ops {
		_, exists := schemas[op.event]
		if !exists {
			schemas[op.event] = &AnnotatedSchema{EventName: op.event, CreatedTS: op.ts}
		}
		err := ApplyOperation(schemas[op.event], scoop_protocol.Operation{
			Action:         scoop_protocol.Action(op.action),
			ActionMetadata: op.actionMetadata,
			Name:           op.name,
		})
		if err != nil {
			return []AnnotatedSchema{}, fmt.Errorf("applying operation to schema: %v", err)
		}
		if op.version >= schemas[op.event].Version {
			schemas[op.event].Version = op.version
			schemas[op.event].TS = op.ts
			schemas[op.event].UserName = op.userName
		}
	}
	ret := make([]AnnotatedSchema, 0, len(schemas))

	for _, val := range schemas {
		if !val.Dropped {
			ret = append(ret, *val)
		}
	}
	return ret, nil
}

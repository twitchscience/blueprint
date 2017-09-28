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
	schemaQueryWithVersion = `
SELECT event, action, name, version, ordering, action_metadata, ts, user_name
FROM operation
WHERE event = $1
AND version <= $2
ORDER BY version ASC, ordering ASC
`
	allSchemasQuery = `
SELECT event, action, name,  version, ordering, action_metadata, ts, user_name
FROM operation
ORDER BY version ASC, ordering ASC
`
	migrationQuery = `
SELECT action, name, action_metadata, version, ordering
FROM operation
WHERE version > $1
AND version <= $2
AND event = $3
ORDER BY version ASC, ordering ASC
`
	insertOperationsQuery = `INSERT INTO operation
(event, action, name, version, ordering, action_metadata, user_name)
VALUES ($1, $2, $3, $4, $5, $6, $7)
`

	nextVersionQuery = `SELECT max(version) + 1
FROM operation
WHERE event = $1
GROUP BY event`

	allMetadataQuery = `
		SELECT DISTINCT em.event, em.metadata_type, em.metadata_value, em.ts, em.user_name, em.version
		  FROM (
				SELECT event, metadata_type, MAX(version) OVER (PARTITION BY event, metadata_type) AS version
				  FROM event_metadata
			   ) v
		  JOIN event_metadata em
		    ON v.event = em.event
		   AND v.metadata_type = em.metadata_type
		   AND v.version = em.version;`

	insertEventMetadataQuery = `
		INSERT INTO event_metadata (event, metadata_type, metadata_value, user_name, version)
		VALUES ($1, $2, $3, $4, $5);`

	nextEventMetadataVersionQuery = `
		SELECT COALESCE(MAX(version) + 1, 1)
		  FROM event_metadata
		 WHERE event = $1
		   AND metadata_type = $2;`
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
func NewSchemaBackend(db *sql.DB) (BpSchemaBackend, error) {
	s := &schemaBackend{db: db}
	return s, nil
}

// Migration returns the operations necessary to migration `table` from version `to -1` to version `to`
func (s *schemaBackend) Migration(table string, from int, to int) ([]*scoop_protocol.Operation, error) {
	rows, err := s.db.Query(migrationQuery, from, to, table)
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
		err := rows.Scan(&s, &op.Name, &b, &op.Version, &op.Ordering)
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
func (s *schemaBackend) CreateSchema(req *scoop_protocol.Config, user string) *core.WebError {
	exists, err := s.SchemaExists(req.EventName)
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
	return core.NewServerWebError(execFnInTransaction(func(tx *sql.Tx) error {
		row := tx.QueryRow(nextVersionQuery, req.EventName)
		var newVersion int
		err = row.Scan(&newVersion)
		switch {
		case err == sql.ErrNoRows:
			newVersion = 0
		case err != nil:
			return fmt.Errorf("parsing response for version number for %s: %v", req.EventName, err)
		}
		err = insertOperations(tx, ops, newVersion, req.EventName, user)
		if err != nil {
			return fmt.Errorf("inserting operations for %s: %v", req.EventName, err)
		}
		return nil
	}, s.db))
}

// UpdateSchema validates that the update operation is valid and if so, stores
// the operations for this migration to the schema as operations in bpdb. It
// applies the operations in order of delete, add, then renames.
func (s *schemaBackend) UpdateSchema(req *core.ClientUpdateSchemaRequest, user string) *core.WebError {
	schema, err := s.Schema(req.EventName, nil)
	if err != nil {
		return core.NewServerWebErrorf("error getting schema to validate schema update: %v", err)
	}
	if schema == nil {
		return core.NewUserWebError(errors.New("schema does not exist"))
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

	return core.NewServerWebError(execFnInTransaction(func(tx *sql.Tx) error {
		row := tx.QueryRow(nextVersionQuery, req.EventName)
		var newVersion int
		err := row.Scan(&newVersion)
		if err != nil {
			return fmt.Errorf("parsing response for version number for %s: %v", req.EventName, err)
		}
		return insertOperations(tx, ops, newVersion, req.EventName, user)
	}, s.db))
}

// DropSchema drops or requests a drop for a schema, depending on whether it exists according to ingester.
func (s *schemaBackend) DropSchema(schema *AnnotatedSchema, reason string, exists bool, user string) error {
	return execFnInTransaction(func(tx *sql.Tx) error {
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
	}, s.db)
}

// SchemaExists checks if a schema name exists in blueprint already
func (s *schemaBackend) SchemaExists(eventName string) (bool, error) {
	schema, err := s.Schema(eventName, nil)
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

// Schema returns the schema for the table `name`
// The version parameter can be used to request the specific version of the schema (0 to the current version)
// If nil is the argument given for version, then Schema() returns the current version of the schema
func (s *schemaBackend) Schema(name string, version *int) (*AnnotatedSchema, error) {
	var rows *sql.Rows
	var err error
	if version == nil {
		rows, err = s.db.Query(schemaQuery, name)
	} else {
		rows, err = s.db.Query(schemaQueryWithVersion, name, *version)
	}
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
func (s *schemaBackend) AllSchemas() ([]AnnotatedSchema, error) {
	rows, err := s.db.Query(allSchemasQuery)
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

func (s *schemaBackend) UpdateEventMetadata(req *core.ClientUpdateEventMetadataRequest, user string) *core.WebError {
	schema, err := s.Schema(req.EventName, nil)
	if err != nil {
		return core.NewServerWebErrorf("error getting schema to validate event metadata update: %v", err)
	}
	if schema == nil {
		return core.NewUserWebError(errors.New("schema does not exist"))
	}

	return core.NewServerWebError(execFnInTransaction(func(tx *sql.Tx) error {
		newVersion, versionErr := getNextEventMetadataVersion(tx, req.EventName, req.MetadataType)
		if versionErr != nil {
			return versionErr
		}
		return insertEventMetadata(tx, req.EventName, req.MetadataType, req.MetadataValue, user, newVersion)
	}, s.db))
}

func getNextEventMetadataVersion(tx *sql.Tx, eventName string, metadataType scoop_protocol.EventMetadataType) (int, error) {
	var newVersion int
	row := tx.QueryRow(nextEventMetadataVersionQuery, eventName, string(metadataType))
	err := row.Scan(&newVersion)
	if err != nil {
		return 0, fmt.Errorf("parsing response for version number of event %s, metadata type %s: %v", eventName, metadataType, err)
	}
	return newVersion, nil
}

// AllEventMetadata returns all of the current event metadata
func (s *schemaBackend) AllEventMetadata() (*AllEventMetadata, error) {
	allMetadata := make(map[string](map[string]EventMetadataRow))
	rows, err := s.db.Query(allMetadataQuery)
	if err != nil {
		return nil, fmt.Errorf("querying for all metadata: %v", err)
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			logger.WithError(err).Error("closing rows in postgres backend AllEventMetadata")
		}
	}()

	for rows.Next() {
		var row EventMetadataRow
		var eventName string
		var metadataType string

		err := rows.Scan(&eventName, &metadataType, &row.MetadataValue, &row.TS, &row.UserName, &row.Version)
		if err != nil {
			return nil, fmt.Errorf("parsing EventMetadata row: %v", err)
		}

		_, exists := allMetadata[eventName]
		if !exists {
			allMetadata[eventName] = make(map[string]EventMetadataRow)
		}

		allMetadata[eventName][metadataType] = row
	}
	return &AllEventMetadata{Metadata: allMetadata}, nil
}

func insertEventMetadata(tx *sql.Tx, eventName string, metadataType scoop_protocol.EventMetadataType, value string, user string, version int) error {
	if _, err := tx.Exec(insertEventMetadataQuery, eventName, string(metadataType), value, user, version); err != nil {
		return fmt.Errorf("INSERTing event_metadata row on %s: %v", eventName, err)
	}
	return nil
}

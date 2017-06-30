package bpdb

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/lib/pq" // to include the 'postgres' driver
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

var (
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

	eventMetadataQuery = `
		SELECT DISTINCT em.event, em.metadata_type, em.metadata_value, em.ts, em.user_name, em.version
		  FROM (
				SELECT event, metadata_type, MAX(version) OVER (PARTITION BY metadata_type) AS version
				  FROM event_metadata
			  	 WHERE event = $1
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

type eventMetadataBackend struct {
	db *sql.DB
}

// NewEventMetadataBackend creates a postgres bpdb backend to interface with
// the kinesis configuration store
func NewEventMetadataBackend(db *sql.DB) BpEventMetadataBackend {
	return &eventMetadataBackend{db: db}
}

func insertEventMetadata(tx *sql.Tx, eventName string, metadataType scoop_protocol.EventMetadataType, value string, user string, version int) error {
	if _, err := tx.Exec(insertEventMetadataQuery, eventName, string(metadataType), value, user, version); err != nil {
		return fmt.Errorf("INSERTing event_metadata row on %s: %v", eventName, err)
	}
	return nil
}

// AllEventMetadata returns all of the current event metadata
func (p *eventMetadataBackend) AllEventMetadata() (*AllEventMetadata, error) {
	// allMetadata := AllEventMetadata{
	allMetadata := make(map[string](map[string]EventMetadataRow))
	rows, err := p.db.Query(allMetadataQuery)
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
		// } else {
		// var metadata = EventMetadata{
		// 	EventName: eventName,
		// 	Metadata:  []EventMetadataRow{row},
		// }
		// allMetadata[eventName] = &metadata

		allMetadata[eventName][metadataType] = row
	}

	// ret := make([]EventMetadata, 0, len(allMetadata))
	// for _, val := range allMetadata {
	// 	ret = append(ret, *val)
	// }
	return &AllEventMetadata{Metadata: allMetadata}, nil
}

// EventMetadata returns the current metadata for an event
// func (p *eventMetadataBackend) EventMetadata(eventName string) (*EventMetadata, error) {
// 	rows, err := p.db.Query(eventMetadataQuery, eventName)
// 	if err != nil {
// 		return nil, fmt.Errorf("querying metadata for event %s: %v", eventName, err)
// 	}

// 	defer func() {
// 		defererr := rows.Close()
// 		if defererr != nil {
// 			logger.WithError(defererr).Error("closing rows in postgres backend Migration")
// 		}
// 	}()

// 	metadata := []EventMetadataRow{}
// 	for rows.Next() {
// 		var row EventMetadataRow
// 		var event string
// 		var metadataType string

// 		err := rows.Scan(&event, &metadataType, &row.MetadataValue, &row.TS, &row.UserName, &row.Version)
// 		if err != nil {
// 			return nil, fmt.Errorf("parsing operation row: %v", err)
// 		}
// 		metadata = append(metadata, row)
// 	}

// 	eventMetadata := EventMetadata{
// 		EventName: eventName,
// 		Metadata:  metadata,
// 	}
// 	return &eventMetadata, nil
// }

func getNextEventMetadataVersion(tx *sql.Tx, eventName string, metadataType scoop_protocol.EventMetadataType) (int, error) {
	var newVersion int
	row := tx.QueryRow(nextEventMetadataVersionQuery, eventName, string(metadataType))
	err := row.Scan(&newVersion)
	if err != nil {
		return 0, fmt.Errorf("parsing response for version number of event %s, metadata type %s: %v", eventName, metadataType, err)
	}
	return newVersion, nil
}

func (p *eventMetadataBackend) UpdateEventMetadata(req *core.ClientUpdateEventMetadataRequest, user string) *core.WebError {
	bpSchemaBackend := NewSchemaBackend(p.db)
	schema, err := bpSchemaBackend.Schema(req.EventName)
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
	}, p.db))
}

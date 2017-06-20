package bpdb

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/lib/pq" // to include the 'postgres' driver
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/blueprint/core"
)

var (
	eventCommentQuery = `
SELECT event, comment, ts, user_name, comment_version
FROM event_comment
WHERE event = $1
ORDER BY comment_version DESC LIMIT 1
`

	insertEventCommentQuery = `
INSERT INTO event_comment
(event, comment, user_name, comment_version)
VALUES ($1, $2, $3, $4)
`

	nextEventCommentVersionQuery = `
SELECT coalesce(max(comment_version) + 1, 1)
FROM event_comment
WHERE event = $1
`
)

type eventCommentBackend struct {
	db *sql.DB
}

// NewEventCommentBackend creates a postgres bpdb backend to interface with
// the kinesis configuration store
func NewEventCommentBackend(db *sql.DB) BpEventCommentBackend {
	return &eventCommentBackend{db: db}
}

func insertEventComment(tx *sql.Tx, comment string, version int, eventName string, user string) error {
	if _, err := tx.Exec(insertEventCommentQuery, eventName, comment, user, version); err != nil {
		return fmt.Errorf("INSERTing event_comment row on %s: %v", eventName, err)
	}
	return nil
}

// EventComment returns the current schema for the table `name`
func (p *eventCommentBackend) EventComment(name string) (*EventComment, error) {
	rows, err := p.db.Query(eventCommentQuery, name)
	if err != nil {
		return nil, fmt.Errorf("querying comment for event %s: %v", name, err)
	}

	defer func() {
		defererr := rows.Close()
		if defererr != nil {
			logger.WithError(defererr).Error("closing rows in postgres backend Migration")
		}
	}()

	if !rows.Next() {
		return nil, fmt.Errorf("no comment found for event %s", name)
	}

	var eventComment EventComment
	err = rows.Scan(&eventComment.EventName,
		&eventComment.Comment,
		&eventComment.TS,
		&eventComment.UserName,
		&eventComment.Version)
	if err != nil {
		return nil, fmt.Errorf("parsing event comment row: %v", err)
	}
	return &eventComment, nil
}

func getNextEventCommentVersion(tx *sql.Tx, eventName string) (int, error) {
	row := tx.QueryRow(nextEventCommentVersionQuery, eventName)

	var newVersion int
	err := row.Scan(&newVersion)

	if err != nil {
		return 0, fmt.Errorf("parsing response for version number of comment for %s: %v", eventName, err)
	}
	return newVersion, nil
}

func (p *eventCommentBackend) UpdateEventComment(req *core.ClientUpdateEventCommentRequest, user string) *core.WebError {
	bpSchemaBackend := NewSchemaBackend(p.db)
	schema, err := bpSchemaBackend.Schema(req.EventName)
	if err != nil {
		return core.NewServerWebErrorf("error getting schema to validate event comment update: %v", err)
	}
	if schema == nil {
		return core.NewUserWebError(errors.New("schema does not exist"))
	}

	return core.NewServerWebError(execFnInTransaction(func(tx *sql.Tx) error {
		newVersion, versionErr := getNextEventCommentVersion(tx, req.EventName)
		if versionErr != nil {
			return versionErr
		}
		return insertEventComment(tx, req.EventComment, newVersion, req.EventName, user)
	}, p.db))
}

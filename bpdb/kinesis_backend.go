package bpdb

import (
	"database/sql"
	"errors"
	"fmt"

	"encoding/json"

	_ "github.com/lib/pq" // to include the 'postgres' driver
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/blueprint/core"
)

var (
	allKinesisConfigsQuery = `
WITH latest_version AS (
	SELECT stream_name, stream_type, aws_account, max(version) as version
	FROM kinesis_config
	GROUP BY stream_name, stream_type, aws_account
)
SELECT kc.stream_name, kc.stream_type, kc.team, kc.version, kc.contact, kc.usage, kc.aws_account, kc.consuming_library, kc.spade_config, kc.last_edited_at, kc.last_changed_by, kc.dropped, kc.dropped_reason
FROM kinesis_config kc
	JOIN latest_version lv
		ON kc.stream_name = lv.stream_name AND kc.stream_type = lv.stream_type AND kc.aws_account = lv.aws_account AND kc.version = lv.version
WHERE NOT dropped
`
	kinesisConfigQuery = `
SELECT stream_name, stream_type, team, version, contact, usage, aws_account, consuming_library, spade_config, last_edited_at, last_changed_by, dropped, dropped_reason
FROM kinesis_config
WHERE aws_account = $1 AND stream_type = $2 AND stream_name = $3 AND NOT dropped
ORDER BY version DESC
LIMIT 1
`
	nextKinesisConfigVersionQuery = `
SELECT max(version) + 1
FROM kinesis_config
WHERE aws_account = $1 AND stream_type = $2 AND stream_name = $3 AND NOT dropped
GROUP BY aws_account, stream_type, stream_name
`
	insertKinesisConfigQuery = `
INSERT INTO kinesis_config
(stream_name, stream_type, team, version, contact, usage, aws_account, consuming_library, spade_config, last_changed_by)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
`
	dropKinesisConfigQuery = `
INSERT INTO kinesis_config
(stream_name, stream_type, aws_account, version, last_changed_by, dropped, dropped_reason)
VALUES ($1, $2, $3, $4, $5, true, $6)
`
)

type kinesisConfigBackend struct {
	db *sql.DB
}

// NewKinesisConfigBackend creates a postgres bpdb backend to interface with
// the kinesis configuration store
func NewKinesisConfigBackend(db *sql.DB) BpKinesisConfigBackend {
	return &kinesisConfigBackend{db: db}
}

//execFnInTransaction takes a closure function of a request and runs it on the db in a transaction
func (p *kinesisConfigBackend) execFnInTransaction(work func(*sql.Tx) error) error {
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

// Schema returns all of the current Kinesis configs
func (p *kinesisConfigBackend) AllKinesisConfigs() ([]AnnotatedKinesisConfig, error) {
	rows, err := p.db.Query(allKinesisConfigsQuery)
	if err != nil {
		return nil, fmt.Errorf("querying for all Kinesis configs: %v", err)
	}
	configs := []AnnotatedKinesisConfig{}
	defer func() {
		err := rows.Close()
		if err != nil {
			logger.WithError(err).Error("closing rows in postgres backend AllKinesisConfigs")
		}
	}()
	for rows.Next() {
		var config AnnotatedKinesisConfig
		var b []byte
		err := rows.Scan(
			&config.StreamName,
			&config.StreamType,
			&config.Team,
			&config.Version,
			&config.Contact,
			&config.Usage,
			&config.AWSAccount,
			&config.ConsumingLibrary,
			&b,
			&config.LastEditedAt,
			&config.LastChangedBy,
			&config.Dropped,
			&config.DroppedReason)
		if err != nil {
			return nil, fmt.Errorf("parsing Kinesis config row: %v", err)
		}
		err = json.Unmarshal(b, &config.SpadeConfig)
		if err != nil {
			return nil, fmt.Errorf("could not unmarshal config JSON in AllKinesisConfigs: %v", err)
		}
		configs = append(configs, config)
	}
	return configs, nil
}

// KinesisConfig returns the current schema for the kinesis `name`
func (p *kinesisConfigBackend) KinesisConfig(account int64, streamType string, name string) (*AnnotatedKinesisConfig, error) {
	row, err := p.db.Query(kinesisConfigQuery, account, streamType, name)
	if err != nil {
		return nil, fmt.Errorf("querying for Kinesis config %d %s %s: %v", account, streamType, name, err)
	}
	if !row.Next() {
		return nil, nil
	}
	var config AnnotatedKinesisConfig
	var b []byte
	err = row.Scan(
		&config.StreamName,
		&config.StreamType,
		&config.Team,
		&config.Version,
		&config.Contact,
		&config.Usage,
		&config.AWSAccount,
		&config.ConsumingLibrary,
		&b,
		&config.LastEditedAt,
		&config.LastChangedBy,
		&config.Dropped,
		&config.DroppedReason)
	if err != nil {
		return nil, fmt.Errorf("parsing Kinesis config row: %v", err)
	}
	err = json.Unmarshal(b, &config.SpadeConfig)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal config JSON in KinesisConfig: %v", err)
	}
	if err != nil {
		return nil, fmt.Errorf("parsing Kinesis config row %d %s %s: %v", account, streamType, name, err)
	}
	return &config, nil
}

// UpdateKinesisConfig validates the updated configuration, then adds it to the database
func (p *kinesisConfigBackend) UpdateKinesisConfig(req *AnnotatedKinesisConfig, user string) *core.WebError {
	config, err := p.KinesisConfig(req.AWSAccount, req.StreamType, req.StreamName)
	if err != nil {
		return core.NewServerWebErrorf("error getting Kinesis config to validate schema update: %v", err)
	}
	if config == nil {
		return core.NewUserWebError(errors.New("Unknown Kinesis configuration"))
	}
	requestErr := validateKinesisConfig(req)
	if requestErr != nil {
		return core.NewUserWebError(requestErr)
	}

	return core.NewServerWebError(p.execFnInTransaction(func(tx *sql.Tx) error {
		row := tx.QueryRow(nextKinesisConfigVersionQuery, req.AWSAccount, req.StreamType, req.StreamName)
		var newVersion int
		err := row.Scan(&newVersion)
		if err != nil {
			return fmt.Errorf("parsing response for version number for %s: %v", req.StreamName, err)
		}
		var b []byte
		b, err = json.Marshal(req.SpadeConfig)
		if err != nil {
			return fmt.Errorf("marshalling %s config to json: %v", req.StreamName, err)
		}
		_, err = tx.Exec(insertKinesisConfigQuery,
			req.StreamName,
			req.StreamType,
			req.Team,
			newVersion,
			req.Contact,
			req.Usage,
			req.AWSAccount,
			req.ConsumingLibrary,
			b, // the marshalled config
			user,
		)
		if err != nil {
			rollErr := tx.Rollback()
			if rollErr != nil {
				return fmt.Errorf("failed to commit: %v; then error rolling back commit: %v", err, rollErr)
			}
			return fmt.Errorf("INSERTing Kinesis config row on %s: %v", req.StreamName, err)
		}
		return nil
	}))
}

// CreateKinesisConfig validates that the creation request is valid and if so, stores
// the Kinesisconfig in bpdb
func (p *kinesisConfigBackend) CreateKinesisConfig(req *AnnotatedKinesisConfig, user string) *core.WebError {
	existing, err := p.KinesisConfig(req.AWSAccount, req.StreamType, req.StreamName)
	if err != nil {
		return core.NewServerWebErrorf("checking for Kinesis config existence: %v", err)
	}
	if existing != nil {
		return core.NewUserWebErrorf("Kinesis configuration already exists")
	}
	requestErr := validateKinesisConfig(req)
	if requestErr != nil {
		return core.NewUserWebError(requestErr)
	}

	return core.NewServerWebError(p.execFnInTransaction(func(tx *sql.Tx) error {
		var b []byte
		b, err := json.Marshal(req.SpadeConfig)
		if err != nil {
			return fmt.Errorf("marshalling %s Kinesis config json: %v", req.StreamName, err)
		}
		_, err = tx.Exec(insertKinesisConfigQuery,
			req.StreamName,
			req.StreamType,
			req.Team,
			0, // first version is always 0
			req.Contact,
			req.Usage,
			req.AWSAccount,
			req.ConsumingLibrary,
			b, // the marshalled config
			user,
		)
		if err != nil {
			rollErr := tx.Rollback()
			if rollErr != nil {
				return fmt.Errorf("failed to commit: %v; then error rolling back commit: %v", err, rollErr)
			}
			return fmt.Errorf("INSERTing Kinesis config on %s: %v", req.StreamName, err)
		}
		return nil
	}))
}

// DropKinesisConfig drops Kinesis config; don't worry, it's recoverable.
func (p *kinesisConfigBackend) DropKinesisConfig(config *AnnotatedKinesisConfig, reason string, user string) error {
	return p.execFnInTransaction(func(tx *sql.Tx) error {
		var newVersion int
		row := tx.QueryRow(nextKinesisConfigVersionQuery, config.AWSAccount, config.StreamType, config.StreamName)
		err := row.Scan(&newVersion)
		if err != nil {
			return fmt.Errorf("parsing response for version number for %s: %v", config.StreamName, err)
		}
		_, err = tx.Exec(dropKinesisConfigQuery,
			config.StreamName,
			config.StreamType,
			config.AWSAccount,
			newVersion,
			user,
			reason,
		)
		if err != nil {
			rollErr := tx.Rollback()
			if rollErr != nil {
				return fmt.Errorf("failed to commit: %v; then error rolling back commit: %v", err, rollErr)
			}
			return fmt.Errorf("INSERTing tombstone row on %s: %v", config.StreamName, err)
		}
		return nil
	})
}

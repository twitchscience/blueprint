package bpdb

import (
	"database/sql"
	"errors"
	"fmt"

	"encoding/json"

	"github.com/aws/aws-sdk-go/aws/session"
	_ "github.com/lib/pq" // to include the 'postgres' driver
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

var (
	allKinesisConfigsQuery = `
WITH latest_version AS (
	SELECT stream_name, stream_type, aws_account, max(version) as version
	FROM kinesis_config
	GROUP BY stream_name, stream_type, aws_account
)
SELECT
	kc.id, kc.team, kc.version, kc.contact, kc.usage, kc.aws_account, kc.consuming_library,
	kc.spade_config, kc.last_edited_at, kc.last_changed_by, kc.dropped, kc.dropped_reason
FROM kinesis_config kc
	JOIN latest_version lv
		ON kc.stream_name = lv.stream_name AND kc.stream_type = lv.stream_type AND kc.aws_account = lv.aws_account AND kc.version = lv.version
WHERE NOT dropped
`
	kinesisConfigQuery = `
SELECT
	id, team, version, contact, usage, aws_account, consuming_library,
	spade_config, last_edited_at, last_changed_by, dropped, dropped_reason
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
(stream_name, stream_type, stream_region, team, version, contact, usage, aws_account, consuming_library, spade_config, last_changed_by)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
`
	updateKinesisConfigQuery = `
INSERT INTO kinesis_config
(id, stream_name, stream_type, stream_region, team, version, contact, usage, aws_account, consuming_library, spade_config, last_changed_by)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
`
	dropKinesisConfigQuery = `
INSERT INTO kinesis_config
(id, stream_name, stream_type, stream_region, aws_account, version, last_changed_by, dropped, dropped_reason)
VALUES ($1, $2, $3, $4, $5, $6, $7, true, $8)
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

// Schema returns all of the current Kinesis configs
func (p *kinesisConfigBackend) AllKinesisConfigs() ([]scoop_protocol.AnnotatedKinesisConfig, error) {
	rows, err := p.db.Query(allKinesisConfigsQuery)
	if err != nil {
		return nil, fmt.Errorf("querying for all Kinesis configs: %v", err)
	}
	configs := []scoop_protocol.AnnotatedKinesisConfig{}
	defer func() {
		err := rows.Close()
		if err != nil {
			logger.WithError(err).Error("closing rows in postgres backend AllKinesisConfigs")
		}
	}()
	for rows.Next() {
		var config scoop_protocol.AnnotatedKinesisConfig
		var b []byte
		err := rows.Scan(
			&config.ID,
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
func (p *kinesisConfigBackend) KinesisConfig(account int64, streamType string, name string) (*scoop_protocol.AnnotatedKinesisConfig, error) {
	row, err := p.db.Query(kinesisConfigQuery, account, streamType, name)
	if err != nil {
		return nil, fmt.Errorf("querying for Kinesis config %d %s %s: %v", account, streamType, name, err)
	}
	if !row.Next() {
		return nil, nil
	}
	var config scoop_protocol.AnnotatedKinesisConfig
	var b []byte
	err = row.Scan(
		&config.ID,
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
func (p *kinesisConfigBackend) UpdateKinesisConfig(req *scoop_protocol.AnnotatedKinesisConfig, user string) *core.WebError {
	config, err := p.KinesisConfig(req.AWSAccount, req.SpadeConfig.StreamType, req.SpadeConfig.StreamName)
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

	return core.NewServerWebError(execFnInTransaction(func(tx *sql.Tx) error {
		row := tx.QueryRow(nextKinesisConfigVersionQuery, req.AWSAccount, req.SpadeConfig.StreamType, req.SpadeConfig.StreamName)
		var newVersion int
		err := row.Scan(&newVersion)
		if err != nil {
			return fmt.Errorf("parsing response for version number for %s: %v", req.SpadeConfig.StreamName, err)
		}
		var b []byte
		b, err = json.Marshal(req.SpadeConfig)
		if err != nil {
			return fmt.Errorf("marshalling %s config to json: %v", req.SpadeConfig.StreamName, err)
		}
		_, err = tx.Exec(updateKinesisConfigQuery,
			req.ID,
			req.SpadeConfig.StreamName,
			req.SpadeConfig.StreamType,
			req.SpadeConfig.StreamRegion,
			req.Team,
			newVersion,
			req.Contact,
			req.Usage,
			req.AWSAccount,
			req.ConsumingLibrary,
			b, // the marshalled config
			user,
		)
		return err
	}, p.db))
}

// CreateKinesisConfig validates that the creation request is valid and if so, stores
// the Kinesisconfig in bpdb
func (p *kinesisConfigBackend) CreateKinesisConfig(req *scoop_protocol.AnnotatedKinesisConfig, user string) *core.WebError {
	existing, err := p.KinesisConfig(req.AWSAccount, req.SpadeConfig.StreamType, req.SpadeConfig.StreamName)
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
	// Set empty region to default region.
	if req.SpadeConfig.StreamRegion == "" {
		sess, err := session.NewSession()
		if err != nil {
			return core.NewServerWebErrorf("creating AWS session: %v", err)
		}
		req.SpadeConfig.StreamRegion = *sess.Config.Region
	}

	return core.NewServerWebError(execFnInTransaction(func(tx *sql.Tx) error {
		var b []byte
		b, err := json.Marshal(req.SpadeConfig)
		if err != nil {
			return fmt.Errorf("marshalling %s Kinesis config json: %v", req.SpadeConfig.StreamName, err)
		}
		_, err = tx.Exec(insertKinesisConfigQuery,
			req.SpadeConfig.StreamName,
			req.SpadeConfig.StreamType,
			req.SpadeConfig.StreamRegion,
			req.Team,
			0, // first version is always 0
			req.Contact,
			req.Usage,
			req.AWSAccount,
			req.ConsumingLibrary,
			b, // the marshalled config
			user,
		)
		return err
	}, p.db))
}

// DropKinesisConfig drops Kinesis config; don't worry, it's recoverable.
func (p *kinesisConfigBackend) DropKinesisConfig(config *scoop_protocol.AnnotatedKinesisConfig, reason string, user string) error {
	return execFnInTransaction(func(tx *sql.Tx) error {
		var newVersion int
		row := tx.QueryRow(nextKinesisConfigVersionQuery, config.AWSAccount, config.SpadeConfig.StreamType, config.SpadeConfig.StreamName)
		err := row.Scan(&newVersion)
		if err != nil {
			return fmt.Errorf("parsing response for version number for %s: %v", config.SpadeConfig.StreamName, err)
		}
		_, err = tx.Exec(dropKinesisConfigQuery,
			config.ID,
			config.SpadeConfig.StreamName,
			config.SpadeConfig.StreamType,
			config.SpadeConfig.StreamRegion,
			config.AWSAccount,
			newVersion,
			user,
			reason,
		)
		return err
	}, p.db)
}

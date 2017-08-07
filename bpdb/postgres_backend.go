package bpdb

import (
	"database/sql"
	"fmt"
	"time"

	"sync"

	_ "github.com/lib/pq" // to include the 'postgres' driver
	"github.com/twitchscience/aws_utils/logger"
)

var (
	getMaintenanceModeQuery = `SELECT is_maintenance, user FROM global_maintenance ORDER BY ts DESC LIMIT 1`
	setMaintenanceModeQuery = `INSERT INTO global_maintenance (is_maintenance, "user", reason) VALUES ($1, $2, $3)`

	getSchemaMaintenanceModesQuery = `SELECT schema, is_maintenance, "user" FROM schema_maintenance
WHERE (schema, ts) IN (SELECT schema, MAX(ts) FROM schema_maintenance GROUP BY schema)`
	setSchemaMaintenanceModeQuery = `INSERT INTO schema_maintenance (schema, is_maintenance, "user", reason) VALUES ($1, $2, $3, $4)`

	dailyChangesLast30Days = `
WITH changes AS (
    SELECT event, version, user_name, MIN(ts) AS ts FROM operation GROUP BY event, version, user_name
)
SELECT DATE_TRUNC('day', "ts") AS day, COUNT(*) as cnt, COUNT(DISTINCT user_name) AS distinct_users
FROM changes
WHERE ts > (CURRENT_DATE - 30)
GROUP BY day
ORDER BY day DESC`

	activeUsersLast30Days = `
WITH changes AS (
    SELECT event, version, user_name, min(ts) AS ts FROM operation GROUP BY event, version, user_name
)
SELECT user_name, COUNT(*) AS event_changes
FROM changes
WHERE ts > (CURRENT_DATE - 30)
GROUP BY user_name
ORDER BY event_changes DESC`

	maintenanceCacheTimeout = time.Duration(15 * time.Second) // time.Duration(10 * time.Minute)
)

type postgresBackend struct {
	db                          *sql.DB
	globalMaintenanceMode       MaintenanceMode
	maintenanceMutex            *sync.RWMutex
	schemaMaintenanceMode       map[string]MaintenanceMode
	schemaMaintenanceLastPulled time.Time
}

// NewPostgresBackend creates a postgres bpdb backend to interface with
// the maintenance mode and stats store
func NewPostgresBackend(db *sql.DB) (Bpdb, error) {
	p := &postgresBackend{
		db:                    db,
		maintenanceMutex:      &sync.RWMutex{},
		schemaMaintenanceMode: make(map[string]MaintenanceMode),
	}
	logger.Info("Querying DB for maintenance mode")
	if err := p.readMaintenanceMode(); err != nil {
		return nil, fmt.Errorf("querying maintenance status: %v", err)
	}
	if err := p.readSchemaMaintenanceModes(); err != nil {
		return nil, fmt.Errorf("querying maintenance status: %v", err)
	}
	return p, nil
}

// readSchemaMaintenanceModes initializes the schema maintenance modes and mutexes by reading from the db
func (p *postgresBackend) readSchemaMaintenanceModes() error {
	p.maintenanceMutex.Lock()
	defer p.maintenanceMutex.Unlock()

	rows, err := p.db.Query(getSchemaMaintenanceModesQuery)
	if err != nil {
		return fmt.Errorf("querying schema maintenance modes: %v", err)
	}
	defer func() {
		defererr := rows.Close()
		if defererr != nil {
			logger.WithError(defererr).Error("closing rows from schema maintenance")
		}
	}()
	for rows.Next() {
		var mode struct {
			schema            string
			inMaintenanceMode bool
			user              string
		}
		err = rows.Scan(&mode.schema, &mode.inMaintenanceMode, &mode.user)
		if err != nil {
			return fmt.Errorf("scanning schema maintenance mode: %v", err)
		}
		p.schemaMaintenanceMode[mode.schema] = MaintenanceMode{IsInMaintenanceMode: mode.inMaintenanceMode, User: mode.user}
	}
	p.schemaMaintenanceLastPulled = time.Now()
	return nil
}

// GetSchemaMaintenanceMode returns true and the user that triggered it if the schema is in
// maintenance mode, else false and an empty string
func (p *postgresBackend) GetSchemaMaintenanceMode(schema string) (MaintenanceMode, error) {
	p.maintenanceMutex.RLock()
	defer p.maintenanceMutex.RUnlock()
	if time.Since(p.schemaMaintenanceLastPulled) > maintenanceCacheTimeout {
		if err := p.readMaintenanceMode(); err != nil {
			return MaintenanceMode{}, fmt.Errorf("querying maintenance status: %v", err)
		}
	}
	return p.schemaMaintenanceMode[schema], nil
}

// SetSchemaMaintenanceMode sets the maintenance mode for the given schema
func (p *postgresBackend) SetSchemaMaintenanceMode(schema string, switchingOn bool, user, reason string) error {
	p.maintenanceMutex.Lock()
	defer p.maintenanceMutex.Unlock()
	if _, err := p.db.Exec(setSchemaMaintenanceModeQuery, schema, switchingOn, user, reason); err != nil {
		return fmt.Errorf("storing schema maintenance mode for %s in db: %v", schema, err)
	}

	p.schemaMaintenanceMode[schema] = MaintenanceMode{IsInMaintenanceMode: switchingOn, User: user}
	return nil
}

func (p *postgresBackend) readMaintenanceMode() error {
	p.maintenanceMutex.Lock()
	defer p.maintenanceMutex.Unlock()
	return p.db.QueryRow(getMaintenanceModeQuery).Scan(&p.globalMaintenanceMode.IsInMaintenanceMode, &p.globalMaintenanceMode.User)
}

func (p *postgresBackend) GetMaintenanceMode() MaintenanceMode {
	p.maintenanceMutex.RLock()
	defer p.maintenanceMutex.RUnlock()
	return p.globalMaintenanceMode
}

func (p *postgresBackend) SetMaintenanceMode(switchingOn bool, user, reason string) error {
	p.maintenanceMutex.Lock()
	defer p.maintenanceMutex.Unlock()

	if _, err := p.db.Exec(setMaintenanceModeQuery, switchingOn, user, reason); err != nil {
		return fmt.Errorf("setting maintenance mode: %v", err)
	}

	p.globalMaintenanceMode = MaintenanceMode{IsInMaintenanceMode: switchingOn, User: user}
	return nil
}

// ActiveUsersLiast30Days lists active users with number of changes made for the last 30 days
func (p *postgresBackend) ActiveUsersLast30Days() ([]*ActiveUser, error) {
	rows, err := p.db.Query(activeUsersLast30Days)
	if err != nil {
		return nil, fmt.Errorf("querying active users over last 30 days: %v", err)
	}
	activeUsers := []*ActiveUser{}
	defer func() {
		err := rows.Close()
		if err != nil {
			logger.WithError(err).Error("closing rows in postgres backend ActiveUsersLast30Days")
		}
	}()
	for rows.Next() {
		var activeUser ActiveUser
		err := rows.Scan(&activeUser.UserName, &activeUser.Changes)
		if err != nil {
			return nil, fmt.Errorf("parsing active user row: %v", err)
		}
		activeUsers = append(activeUsers, &activeUser)
	}
	return activeUsers, nil
}

// DailyChangesLast30Days lists number of changes and number of users making them per day for the last 30 days
func (p *postgresBackend) DailyChangesLast30Days() ([]*DailyChange, error) {
	rows, err := p.db.Query(dailyChangesLast30Days)
	if err != nil {
		return nil, fmt.Errorf("querying daily changes over last 30 days: %v", err)
	}
	dailyChanges := []*DailyChange{}
	defer func() {
		err := rows.Close()
		if err != nil {
			logger.WithError(err).Error("closing rows in postgres backend DailyChangesLast30Days")
		}
	}()
	for rows.Next() {
		var dailyChange DailyChange
		err := rows.Scan(&dailyChange.Day, &dailyChange.Changes, &dailyChange.Users)
		if err != nil {
			return nil, fmt.Errorf("parsing change row: %v", err)
		}
		dailyChanges = append(dailyChanges, &dailyChange)
	}
	return dailyChanges, nil
}

package bpdb

import (
	"database/sql"
	"fmt"

	"sync"

	_ "github.com/lib/pq" // to include the 'postgres' driver
	"github.com/twitchscience/aws_utils/logger"
)

var (
	getMaintenanceModeQuery = `SELECT is_maintenance FROM maintenance ORDER BY ts DESC LIMIT 1`

	setMaintenanceModeQuery = `INSERT INTO maintenance (is_maintenance, "user", reason) VALUES ($1, $2, $3)`

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
)

type postgresBackend struct {
	db                *sql.DB
	inMaintenanceMode bool
	maintenanceMutex  *sync.RWMutex
}

// NewPostgresBackend creates a postgres bpdb backend to interface with
// the maintenance mode and stats store
func NewPostgresBackend(db *sql.DB) (Bpdb, error) {
	p := &postgresBackend{db: db, maintenanceMutex: &sync.RWMutex{}}
	logger.Info("Querying DB for maintenance mode")
	if err := p.readMaintenanceMode(); err != nil {
		return nil, fmt.Errorf("querying maintenance status: %v", err)
	}
	logger.WithField("is_maintenance", p.IsInMaintenanceMode()).Info("Got maintenance mode from DB")

	return p, nil
}

func (p *postgresBackend) readMaintenanceMode() error {
	p.maintenanceMutex.Lock()
	defer p.maintenanceMutex.Unlock()
	return p.db.QueryRow(getMaintenanceModeQuery).Scan(&p.inMaintenanceMode)
}

func (p *postgresBackend) IsInMaintenanceMode() bool {
	p.maintenanceMutex.RLock()
	defer p.maintenanceMutex.RUnlock()
	return p.inMaintenanceMode
}

func (p *postgresBackend) SetMaintenanceMode(switchingOn bool, user, reason string) error {
	p.maintenanceMutex.Lock()
	defer p.maintenanceMutex.Unlock()

	if _, err := p.db.Exec(setMaintenanceModeQuery, switchingOn, user, reason); err != nil {
		return fmt.Errorf("setting maintenance mode: %v", err)
	}

	p.inMaintenanceMode = switchingOn
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

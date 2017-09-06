package bpdb

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // to include the 'postgres' driver
)

// execFnInTransaction takes a closure function of a request and runs it on the db in a transaction
func execFnInTransaction(work func(*sql.Tx) error, db *sql.DB) error {
	tx, err := db.Begin()
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
	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("failed in commit: %v", commitErr)
	}
	return nil
}

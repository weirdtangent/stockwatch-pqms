package main

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type LastDone struct {
	Activity         string       `db:"activity"`
	UniqueKey        string       `db:"unique_key"`
	LastStatus       string       `db:"last_status"`
	LastDoneDatetime sql.NullTime `db:"lastdone_datetime"`
	CreateDatetime   sql.NullTime `db:"create_datetime"`
	UpdateDatetime   sql.NullTime `db:"update_datetime"`
}

// object methods -------------------------------------------------------------
func (ld *LastDone) getByActivity(db *sqlx.DB) error {
	err := db.QueryRowx("SELECT * FROM lastdone WHERE activity=? AND unique_key=?", ld.Activity, ld.UniqueKey).StructScan(ld)
	return err
}

func (ld *LastDone) createOrUpdate(db *sqlx.DB) error {
	var command = "INSERT INTO lastdone (activity, unique_key, last_status, lastdone_datetime) VALUES(?, ?, ?, ?) ON DUPLICATE KEY UPDATE last_status=?, lastdone_datetime=?"
	_, err := db.Exec(command, ld.Activity, ld.UniqueKey, ld.LastStatus, ld.LastDoneDatetime, ld.LastStatus, ld.LastDoneDatetime)
	if err != nil {
		ld.getByActivity(db)
	}
	return err
}

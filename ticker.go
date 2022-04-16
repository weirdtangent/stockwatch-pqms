package main

import (
	"github.com/jmoiron/sqlx"
)

func (t *Ticker) getById(db *sqlx.DB) error {
	err := db.QueryRowx("SELECT * FROM ticker WHERE ticker_id=?", t.TickerId).StructScan(t)
	return err
}

func (t *Ticker) getBySymbol(db *sqlx.DB) error {
	err := db.QueryRowx("SELECT * FROM ticker WHERE ticker_symbol=?", t.TickerSymbol).StructScan(t)
	return err
}

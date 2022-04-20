package main

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
)

func (t *Ticker) getById(db *sqlx.DB) error {
	err := db.QueryRowx("SELECT * FROM ticker WHERE ticker_id=?", t.TickerId).StructScan(t)
	return err
}

func (t *Ticker) getBySymbol(db *sqlx.DB) error {
	err := db.QueryRowx("SELECT * FROM ticker WHERE ticker_symbol=?", t.TickerSymbol).StructScan(t)
	return err
}

func updateTickerById(ctx context.Context, tickerId int64, performanceId string) error {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)
	logger := log.Ctx(ctx)

	if tickerId == 0 {
		return nil
	}
	var update = "UPDATE ticker SET ms_performance_id=? WHERE ticker_id=?"
	_, err := db.Exec(update, performanceId, tickerId)
	if err != nil {
		logger.Warn().Err(err).
			Str("table_name", "ticker").
			Int64("ticker_id", tickerId).
			Msg("failed on UPDATE")
		return err
	}
	return nil
}

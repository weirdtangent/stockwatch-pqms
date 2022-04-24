package main

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
)

type TickerAttribute struct {
	TickerAttributeId uint64       `db:"attribute_id"`
	TickerId          uint64       `db:"ticker_id"`
	AttributeName     string       `db:"attribute_name"`
	AttributeComment  string       `db:"attribute_comment"`
	AttributeValue    string       `db:"attribute_value"`
	CreateDatetime    sql.NullTime `db:"create_datetime"`
	UpdateDatetime    sql.NullTime `db:"update_datetime"`
}

func (t *Ticker) getById(db *sqlx.DB) error {
	err := db.QueryRowx("SELECT * FROM ticker WHERE ticker_id=?", t.TickerId).StructScan(t)
	return err
}

func (t *Ticker) getBySymbol(db *sqlx.DB) error {
	err := db.QueryRowx("SELECT * FROM ticker WHERE ticker_symbol=?", t.TickerSymbol).StructScan(t)
	return err
}

func updateTickerById(ctx context.Context, tickerId uint64, performanceId string) error {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)

	if tickerId == 0 {
		return nil
	}
	var update = "UPDATE ticker SET ms_performance_id=? WHERE ticker_id=?"
	_, err := db.Exec(update, performanceId, tickerId)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).
			Str("table_name", "ticker").
			Uint64("ticker_id", tickerId).
			Msg("failed on UPDATE")
		return err
	}
	return nil
}

func (t *Ticker) createOrUpdateAttribute(ctx context.Context, attributeName, attributeComment, attributeValue string) error {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)

	attribute := TickerAttribute{0, t.TickerId, attributeName, "", attributeValue, sql.NullTime{}, sql.NullTime{}}
	err := attribute.getByUniqueKey(ctx)
	if err == nil {
		var update = "UPDATE ticker_attribute SET attribute_value=? WHERE ticker_id=? AND attribute_name=? AND attribute_comment=?"
		db.Exec(update, attributeValue, t.TickerId, attributeName, attributeComment)
		return nil
	}

	var insert = "INSERT INTO ticker_attribute SET ticker_id=?, attribute_name=?, attribute_value=?, attribute_comment=?"
	db.Exec(insert, t.TickerId, attributeName, attributeValue, attributeComment)
	return nil
}

func (ta *TickerAttribute) getByUniqueKey(ctx context.Context) error {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)

	err := db.QueryRowx(`SELECT * FROM ticker_attribute WHERE ticker_id=? AND attribute_name=? AND attribute_comment=?`, ta.TickerId, ta.AttributeName, ta.AttributeComment).StructScan(ta)
	return err
}

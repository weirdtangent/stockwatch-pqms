package main

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
)

type Ticker struct {
	TickerId        uint64       `db:"ticker_id"`
	TickerSymbol    string       `db:"ticker_symbol"`
	ExchangeId      uint64       `db:"exchange_id"`
	TickerName      string       `db:"ticker_name"`
	CompanyName     string       `db:"company_name"`
	Address         string       `db:"address"`
	City            string       `db:"city"`
	State           string       `db:"state"`
	Zip             string       `db:"zip"`
	Country         string       `db:"country"`
	Website         string       `db:"website"`
	Phone           string       `db:"phone"`
	Sector          string       `db:"sector"`
	Industry        string       `db:"industry"`
	FavIconS3Key    string       `db:"favicon_s3key"`
	FetchDatetime   sql.NullTime `db:"fetch_datetime"`
	MSPerformanceId string       `db:"ms_performance_id"`
	CreateDatetime  sql.NullTime `db:"create_datetime"`
	UpdateDatetime  sql.NullTime `db:"update_datetime"`
}

type Exchange struct {
	ExchangeId      uint64       `db:"exchange_id"`
	ExchangeAcronym string       `db:"exchange_acronym"`
	ExchangeMic     string       `db:"exchange_mic"`
	ExchangeName    string       `db:"exchange_name"`
	CountryId       uint64       `db:"country_id"`
	City            string       `db:"city"`
	CreateDatetime  sql.NullTime `db:"create_datetime"`
	UpdateDatetime  sql.NullTime `db:"update_datetime"`
}

type TickerDaily struct {
	TickerDailyId  uint64       `db:"ticker_daily_id"`
	TickerId       uint64       `db:"ticker_id"`
	PriceDate      string       `db:"price_date"`
	OpenPrice      float32      `db:"open_price"`
	HighPrice      float32      `db:"high_price"`
	LowPrice       float32      `db:"low_price"`
	ClosePrice     float32      `db:"close_price"`
	Volume         float32      `db:"volume"`
	CreateDatetime sql.NullTime `db:"create_datetime"`
	UpdateDatetime sql.NullTime `db:"update_datetime"`
}

type TickerAttribute struct {
	TickerAttributeId uint64       `db:"attribute_id"`
	TickerId          uint64       `db:"ticker_id"`
	AttributeName     string       `db:"attribute_name"`
	AttributeComment  string       `db:"attribute_comment"`
	AttributeValue    string       `db:"attribute_value"`
	CreateDatetime    sql.NullTime `db:"create_datetime"`
	UpdateDatetime    sql.NullTime `db:"update_datetime"`
}

func (t *Ticker) getById(ctx context.Context) error {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)

	err := db.QueryRowx("SELECT * FROM ticker WHERE ticker_id=?", t.TickerId).StructScan(t)
	return err
}

func (t *Ticker) getBySymbol(ctx context.Context) error {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)

	err := db.QueryRowx("SELECT * FROM ticker WHERE ticker_symbol=?", t.TickerSymbol).StructScan(t)
	return err
}

func updateTickerPerformanceId(ctx context.Context, tickerId uint64, performanceId string) error {
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

func (t *Ticker) create(ctx context.Context) error {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)

	if t.TickerSymbol == "" {
		// refusing to add ticker with blank symbol
		return nil
	}

	var insert = "INSERT INTO ticker SET ticker_symbol=?, exchange_id=?, ticker_name=?, company_name=?, address=?, city=?, state=?, zip=?, country=?, website=?, phone=?, sector=?, industry=?"
	if !t.FetchDatetime.Valid {
		insert += ", fetch_datetime=now()"
	}
	res, err := db.Exec(insert, t.TickerSymbol, t.ExchangeId, t.TickerName, t.CompanyName, t.Address, t.City, t.State, t.Zip, t.Country, t.Website, t.Phone, t.Sector, t.Industry)
	if err != nil {
		zerolog.Ctx(ctx).Fatal().Err(err).Str("table_name", "ticker").Str("ticker", t.TickerSymbol).Msg("failed on INSERT")
		return err
	}
	tickerId, err := res.LastInsertId()
	if err != nil {
		zerolog.Ctx(ctx).Fatal().Err(err).Str("table_name", "ticker").Str("symbol", t.TickerSymbol).Msg("failed on LAST_INSERTID")
		return err
	}
	t.TickerId = uint64(tickerId)
	return nil
}

func (t *Ticker) createOrUpdate(ctx context.Context) error {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)

	if t.TickerSymbol == "" {
		// refusing to add ticker with blank symbol
		return nil
	}

	if t.TickerId == 0 {
		err := t.getBySymbol(ctx)
		if errors.Is(err, sql.ErrNoRows) || t.TickerId == 0 {
			return t.create(ctx)
		}
	}

	var update = "UPDATE ticker SET exchange_id=?, ticker_name=?, company_name=?, address=?, city=?, state=?, zip=?, country=?, website=?, phone=?, sector=?, industry=?, favicon_s3key=?, fetch_datetime=now() WHERE ticker_id=?"
	_, err := db.Exec(update, t.ExchangeId, t.TickerName, t.CompanyName, t.Address, t.City, t.State, t.Zip, t.Country, t.Website, t.Phone, t.Sector, t.Industry, t.FavIconS3Key, t.TickerId)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Str("table_name", "ticker").Str("symbol", t.TickerSymbol).Msg("failed on update")
	}
	return t.getById(ctx)
}

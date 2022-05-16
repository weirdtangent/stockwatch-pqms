package main

import (
	"database/sql"
	"errors"
	"time"

	"github.com/rs/zerolog"
)

type Ticker struct {
	TickerId            uint64 `db:"ticker_id"`
	EId                 string
	TickerSymbol        string    `db:"ticker_symbol"`
	TickerType          string    `db:"ticker_type"`
	TickerMarket        string    `db:"ticker_market"`
	ExchangeId          uint64    `db:"exchange_id"`
	TickerName          string    `db:"ticker_name"`
	CompanyName         string    `db:"company_name"`
	Address             string    `db:"address"`
	City                string    `db:"city"`
	State               string    `db:"state"`
	Zip                 string    `db:"zip"`
	Country             string    `db:"country"`
	Website             string    `db:"website"`
	Phone               string    `db:"phone"`
	Sector              string    `db:"sector"`
	Industry            string    `db:"industry"`
	MarketPrice         float64   `db:"market_price"`
	MarketPrevClose     float64   `db:"market_prev_close"`
	MarketVolume        int64     `db:"market_volume"`
	MarketPriceDatetime time.Time `db:"market_price_datetime"`
	FavIconS3Key        string    `db:"favicon_s3key"`
	FetchDatetime       time.Time `db:"fetch_datetime"`
	MSPerformanceId     string    `db:"ms_performance_id"`
	CreateDatetime      time.Time `db:"create_datetime"`
	UpdateDatetime      time.Time `db:"update_datetime"`
}

type Exchange struct {
	ExchangeId      uint64 `db:"exchange_id"`
	EId             string
	ExchangeAcronym string       `db:"exchange_acronym"`
	ExchangeMic     string       `db:"exchange_mic"`
	ExchangeName    string       `db:"exchange_name"`
	CountryId       uint64       `db:"country_id"`
	City            string       `db:"city"`
	CreateDatetime  sql.NullTime `db:"create_datetime"`
	UpdateDatetime  sql.NullTime `db:"update_datetime"`
}

type TickerDaily struct {
	TickerDailyId  uint64 `db:"ticker_daily_id"`
	EId            string
	TickerId       uint64    `db:"ticker_id"`
	PriceDatetime  time.Time `db:"price_datetime"`
	OpenPrice      float64   `db:"open_price"`
	HighPrice      float64   `db:"high_price"`
	LowPrice       float64   `db:"low_price"`
	ClosePrice     float64   `db:"close_price"`
	Volume         int64     `db:"volume"`
	CreateDatetime time.Time `db:"create_datetime"`
	UpdateDatetime time.Time `db:"update_datetime"`
}

type TickerAttribute struct {
	TickerAttributeId uint64 `db:"attribute_id"`
	EId               string
	TickerId          uint64    `db:"ticker_id"`
	AttributeName     string    `db:"attribute_name"`
	AttributeComment  string    `db:"attribute_comment"`
	AttributeValue    string    `db:"attribute_value"`
	CreateDatetime    time.Time `db:"create_datetime"`
	UpdateDatetime    time.Time `db:"update_datetime"`
}

type TickerSplit struct {
	TickerSplitId  uint64 `db:"ticker_split_id"`
	EId            string
	TickerId       uint64    `db:"ticker_id"`
	SplitDate      time.Time `db:"split_date"`
	SplitRatio     string    `db:"split_ratio"`
	CreateDatetime time.Time `db:"create_datetime"`
	UpdateDatetime time.Time `db:"update_datetime"`
}

func (t *Ticker) getById(deps *Dependencies) error {
	db := deps.db

	err := db.QueryRowx("SELECT * FROM ticker WHERE ticker_id=?", t.TickerId).StructScan(t)
	return err
}

func (t *Ticker) getBySymbol(deps *Dependencies) error {
	db := deps.db

	err := db.QueryRowx("SELECT * FROM ticker WHERE ticker_symbol=?", t.TickerSymbol).StructScan(t)
	return err
}

func updateTickerPerformanceId(deps *Dependencies, tickerId uint64, performanceId string) error {
	db := deps.db
	sublog := deps.logger

	if tickerId == 0 {
		return nil
	}
	var update = "UPDATE ticker SET ms_performance_id=? WHERE ticker_id=?"
	_, err := db.Exec(update, performanceId, tickerId)
	if err != nil {
		sublog.Warn().Err(err).Str("table_name", "ticker").Uint64("ticker_id", tickerId).Msg("failed on UPDATE")
		return err
	}
	return nil
}

func (t *Ticker) createOrUpdateAttribute(deps *Dependencies, attributeName, attributeComment, attributeValue string) error {
	db := deps.db

	attribute := TickerAttribute{0, "", t.TickerId, attributeName, "", attributeValue, time.Now(), time.Now()}
	err := attribute.getByUniqueKey(deps)
	if err == nil {
		var update = "UPDATE ticker_attribute SET attribute_value=? WHERE ticker_id=? AND attribute_name=? AND attribute_comment=?"
		db.Exec(update, attributeValue, t.TickerId, attributeName, attributeComment)
		return nil
	}

	var insert = "INSERT INTO ticker_attribute SET ticker_id=?, attribute_name=?, attribute_value=?, attribute_comment=?"
	db.Exec(insert, t.TickerId, attributeName, attributeValue, attributeComment)
	return nil
}

func (ta *TickerAttribute) getByUniqueKey(deps *Dependencies) error {
	db := deps.db

	err := db.QueryRowx(`SELECT * FROM ticker_attribute WHERE ticker_id=? AND attribute_name=? AND attribute_comment=?`, ta.TickerId, ta.AttributeName, ta.AttributeComment).StructScan(ta)
	return err
}

func (t *Ticker) getIdBySymbol(deps *Dependencies) (uint64, error) {
	db := deps.db

	var tickerId uint64
	err := db.QueryRowx("SELECT ticker_id FROM ticker WHERE ticker_symbol=?", t.TickerSymbol).Scan(&tickerId)
	return tickerId, err
}

func (t *Ticker) Update(deps *Dependencies, sublog zerolog.Logger) error {
	db := deps.db

	var update = "UPDATE ticker SET ticker_type=?, ticker_market=?, exchange_id=?, ticker_name=?, company_name=?, address=?, city=?, state=?, zip=?, country=?, website=?, phone=?, sector=?, industry=?, market_price=?, market_prev_close=?, market_volume=?, market_price_datetime=?, favicon_s3key=?, fetch_datetime=now() WHERE ticker_id=?"
	_, err := db.Exec(update, t.TickerType, t.TickerMarket, t.ExchangeId, t.TickerName, t.CompanyName, t.Address, t.City, t.State, t.Zip, t.Country, t.Website, t.Phone, t.Sector, t.Industry, t.MarketPrice, t.MarketPrevClose, t.MarketVolume, t.MarketPriceDatetime, t.FavIconS3Key, t.TickerId)
	return err
}

func (t *Ticker) create(deps *Dependencies) error {
	db := deps.db
	sublog := deps.logger

	if t.TickerSymbol == "" {
		// refusing to add ticker with blank symbol
		return nil
	}

	insert := "INSERT INTO ticker SET ticker_symbol=?, ticker_type=?, ticker_market=?, exchange_id=?, ticker_name=?, company_name=?, address=?, city=?, state=?, zip=?, country=?, website=?, phone=?, sector=?, industry=?, market_price=?, market_prev_close=?, market_volume=?, fetch_datetime=?"
	res, err := db.Exec(insert, t.TickerSymbol, t.TickerType, t.TickerMarket, t.ExchangeId, t.TickerName, t.CompanyName, t.Address, t.City, t.State, t.Zip, t.Country, t.Website, t.Phone, t.Sector, t.Industry, t.MarketPrice, t.MarketPrevClose, t.MarketVolume, t.FetchDatetime)
	if err != nil {
		sublog.Fatal().Err(err).Str("ticker", t.TickerSymbol).Msg("failed on INSERT")
		return err
	}
	tickerId, err := res.LastInsertId()
	if err != nil {
		sublog.Fatal().Err(err).Str("symbol", t.TickerSymbol).Msg("failed on LAST_INSERTID")
		return err
	}
	t.TickerId = uint64(tickerId)
	return nil
}

func (t *Ticker) createOrUpdate(deps *Dependencies) error {
	sublog := deps.logger

	if t.TickerSymbol == "" {
		// refusing to add ticker with blank symbol
		return nil
	}

	if t.TickerId == 0 {
		var err error
		t.TickerId, err = t.getIdBySymbol(deps)
		if errors.Is(err, sql.ErrNoRows) || t.TickerId == 0 {
			return t.create(deps)
		}
	}

	t.Update(deps, *sublog)
	return t.getById(deps)
}

func (td *TickerDaily) checkByDate(deps *Dependencies) uint64 {
	db := deps.db

	var tickerDailyId uint64
	db.QueryRowx(`SELECT ticker_daily_id FROM ticker_daily WHERE ticker_id=? AND price_date LIKE ?`, td.TickerId, td.PriceDatetime.Format("2006-01-02%")).Scan(&tickerDailyId)
	return tickerDailyId
}

func (td *TickerDaily) create(deps *Dependencies) error {
	db := deps.db
	sublog := deps.logger

	if td.Volume == 0 {
		// Refusing to add ticker daily with 0 volume
		return nil
	}

	var insert = "INSERT INTO ticker_daily SET ticker_id=?, price_datetime=?, open_price=?, high_price=?, low_price=?, close_price=?, volume=?"
	_, err := db.Exec(insert, td.TickerId, td.PriceDatetime, td.OpenPrice, td.HighPrice, td.LowPrice, td.ClosePrice, td.Volume)
	if err != nil {
		sublog.Fatal().Err(err).Msg("failed on INSERT")
	}
	return err
}

func (td *TickerDaily) createOrUpdate(deps *Dependencies) error {
	db := deps.db
	sublog := deps.logger

	if td.Volume == 0 {
		// Refusing to add ticker daily with 0 volume
		return nil
	}

	td.TickerDailyId = td.checkByDate(deps)
	if td.TickerDailyId == 0 {
		return td.create(deps)
	}

	var update = "UPDATE ticker_daily SET price_datetime=?, open_price=?, high_price=?, low_price=?, close_price=?, volume=? WHERE ticker_id=? AND price_date LIKE ?"
	_, err := db.Exec(update, td.PriceDatetime, td.OpenPrice, td.HighPrice, td.LowPrice, td.ClosePrice, td.Volume, td.TickerId, td.PriceDatetime.Format("2006-01-02%"))
	if err != nil {
		sublog.Warn().Err(err).Msg("failed on UPDATE")
	}
	return err
}

func (ts *TickerSplit) getByDate(deps *Dependencies) error {
	db := deps.db

	err := db.QueryRowx(`SELECT * FROM ticker_split WHERE ticker_id=? AND split_date=?`, ts.TickerId, ts.SplitDate).StructScan(ts)
	return err
}

func (ts *TickerSplit) createIfNew(deps *Dependencies) error {
	db := deps.db
	sublog := deps.logger

	if ts.SplitRatio == "" {
		// Refusing to add ticker split with blank ratio
		return nil
	}

	err := ts.getByDate(deps)
	if err == nil {
		return nil
	}

	var insert = "INSERT INTO ticker_split SET ticker_id=?, split_date=?, split_ratio=?"
	_, err = db.Exec(insert, ts.TickerId, ts.SplitDate, ts.SplitRatio)
	if err != nil {
		sublog.Fatal().Err(err).Msg("failed on INSERT")
	}
	return err
}

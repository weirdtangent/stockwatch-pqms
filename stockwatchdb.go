package main

import (
	//"github.com/aws/aws-sdk-go/aws/session"
	"database/sql"

	"github.com/jmoiron/sqlx"
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
	FetchDatetime   string       `db:"fetch_datetime"`
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

func getTickerById(db *sqlx.DB, tickerId uint64) (*Ticker, error) {
	var ticker Ticker
	err := db.QueryRowx("SELECT * FROM ticker WHERE ticker_id=?", tickerId).StructScan(&ticker)
	return &ticker, err
}

func getExchangeById(db *sqlx.DB, exchange_id uint64) (*Exchange, error) {
	var exchange Exchange
	err := db.QueryRowx("SELECT * FROM exchange WHERE exchange_id = ?", exchange_id).StructScan(&exchange)
	return &exchange, err
}

func getTickerDaily(db *sqlx.DB, ticker_id uint64, daily_date string) (*TickerDaily, error) {
	var tickerdaily TickerDaily
	if len(daily_date) > 10 {
		daily_date = daily_date[0:10]
	}
	err := db.QueryRowx(`SELECT * FROM ticker_daily WHERE ticker_id=? AND price_date=?`, ticker_id, daily_date).StructScan(&tickerdaily)
	return &tickerdaily, err
}

func getTickerDailyById(db *sqlx.DB, ticker_daily_id uint64) (*TickerDaily, error) {
	var tickerdaily TickerDaily
	err := db.QueryRowx(`SELECT * FROM ticker_daily WHERE ticker_daily_id=?`, ticker_daily_id).StructScan(&tickerdaily)
	return &tickerdaily, err
}

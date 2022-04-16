package main

import (
	//"github.com/aws/aws-sdk-go/aws/session"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
)

type Ticker struct {
	TickerId        int64  `db:"ticker_id"`
	TickerSymbol    string `db:"ticker_symbol"`
	ExchangeId      int64  `db:"exchange_id"`
	TickerName      string `db:"ticker_name"`
	CompanyName     string `db:"company_name"`
	Address         string `db:"address"`
	City            string `db:"city"`
	State           string `db:"state"`
	Zip             string `db:"zip"`
	Country         string `db:"country"`
	Website         string `db:"website"`
	Phone           string `db:"phone"`
	Sector          string `db:"sector"`
	Industry        string `db:"industry"`
	FetchDatetime   string `db:"fetch_datetime"`
	MSPerformanceId string `db:"ms_performance_id"`
	CreateDatetime  string `db:"create_datetime"`
	UpdateDatetime  string `db:"update_datetime"`
}

type Exchange struct {
	ExchangeId      int64  `db:"exchange_id"`
	ExchangeAcronym string `db:"exchange_acronym"`
	ExchangeMic     string `db:"exchange_mic"`
	ExchangeName    string `db:"exchange_name"`
	CountryId       int64  `db:"country_id"`
	City            string `db:"city"`
	CreateDatetime  string `db:"create_datetime"`
	UpdateDatetime  string `db:"update_datetime"`
}

type TickerDaily struct {
	TickerDailyId  int64   `db:"ticker_daily_id"`
	TickerId       int64   `db:"ticker_id"`
	PriceDate      string  `db:"price_date"`
	OpenPrice      float32 `db:"open_price"`
	HighPrice      float32 `db:"high_price"`
	LowPrice       float32 `db:"low_price"`
	ClosePrice     float32 `db:"close_price"`
	Volume         float32 `db:"volume"`
	CreateDatetime string  `db:"create_datetime"`
	UpdateDatetime string  `db:"update_datetime"`
}

func getTickerById(db *sqlx.DB, tickerId int64) (*Ticker, error) {
	var ticker Ticker
	err := db.QueryRowx("SELECT * FROM ticker WHERE ticker_id=?", tickerId).StructScan(&ticker)
	return &ticker, err
}

func getExchangeById(db *sqlx.DB, exchange_id int64) (*Exchange, error) {
	var exchange Exchange
	err := db.QueryRowx("SELECT * FROM exchange WHERE exchange_id = ?", exchange_id).StructScan(&exchange)
	return &exchange, err
}

func getTickerDaily(db *sqlx.DB, ticker_id int64, daily_date string) (*TickerDaily, error) {
	var tickerdaily TickerDaily
	if len(daily_date) > 10 {
		daily_date = daily_date[0:10]
	}
	err := db.QueryRowx(`SELECT * FROM ticker_daily WHERE ticker_id=? AND price_date=?`, ticker_id, daily_date).StructScan(&tickerdaily)
	return &tickerdaily, err
}

func getTickerDailyById(db *sqlx.DB, ticker_daily_id int64) (*TickerDaily, error) {
	var tickerdaily TickerDaily
	err := db.QueryRowx(`SELECT * FROM ticker_daily WHERE ticker_daily_id=?`, ticker_daily_id).StructScan(&tickerdaily)
	return &tickerdaily, err
}

func createTickerDaily(db *sqlx.DB, tickerdaily *TickerDaily) (*TickerDaily, error) {
	var insert = "INSERT INTO ticker_daily SET ticker_id=?, price_date=?, open_price=?, high_price=?, low_price=?, close_price=?, volume=?"

	res, err := db.Exec(insert, tickerdaily.TickerId, tickerdaily.PriceDate, tickerdaily.OpenPrice, tickerdaily.HighPrice, tickerdaily.LowPrice, tickerdaily.ClosePrice, tickerdaily.Volume)
	if err != nil {
		log.Fatal().Err(err).
			Str("table_name", "ticker_daily").
			Msg("Failed on INSERT")
	}
	ticker_daily_id, err := res.LastInsertId()
	if err != nil {
		log.Fatal().Err(err).
			Str("table_name", "ticker_daily").
			Msg("Failed on LAST_INSERT_ID")
	}
	return getTickerDailyById(db, ticker_daily_id)
}

func createOrUpdateTickerDaily(db *sqlx.DB, tickerdaily *TickerDaily) (*TickerDaily, error) {
	var update = "UPDATE ticker_daily SET open_price=?, high_price=?, low_price=?, close_price=?, volume=? WHERE ticker_id=? AND price_date=?"

	existing, err := getTickerDaily(db, tickerdaily.TickerId, tickerdaily.PriceDate)
	if err != nil {
		return createTickerDaily(db, tickerdaily)
	}

	_, err = db.Exec(update, tickerdaily.OpenPrice, tickerdaily.HighPrice, tickerdaily.LowPrice, tickerdaily.ClosePrice, tickerdaily.Volume, existing.TickerId, existing.PriceDate)
	if err != nil {
		log.Warn().Err(err).
			Str("table_name", "ticker_daily").
			Msg("Failed on UPDATE")
	}
	return getTickerDailyById(db, existing.TickerDailyId)
}

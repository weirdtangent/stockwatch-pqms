package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type TaskTickerNewsBody struct {
	TickerId     uint64 `json:"ticker_id"`
	TickerSymbol string `json:"ticker_symbol"`
	ExchangeId   uint64 `json:"exchange_id"`
}

const (
	minTickerNewsDelay = 60 * 4 // 4 hours
)

func perform_tickers_news(deps *Dependencies, body *string) (bool, error) {
	db := deps.db
	sublog := deps.logger

	if body == nil || *body == "" {
		sublog.Error().Msg("missing task body")
		return true, fmt.Errorf("missing task body")
	}
	var taskTickerNewsBody TaskTickerNewsBody
	json.NewDecoder(strings.NewReader(*body)).Decode(&taskTickerNewsBody)

	if taskTickerNewsBody.TickerId == 0 && taskTickerNewsBody.TickerSymbol == "" {
		sublog.Error().Msg("tickerId OR tickerSymbol must be provided")
		return true, fmt.Errorf("tickerId OR tickerSymbol must be provided")
	}

	ticker := Ticker{TickerId: taskTickerNewsBody.TickerId, TickerSymbol: taskTickerNewsBody.TickerSymbol}
	var err error
	if ticker.TickerId > 0 {
		err = ticker.getById(deps)
	} else {
		err = ticker.getBySymbol(deps)
	}
	if err != nil {
		sublog.Error().Interface("ticker", ticker).Msg("couldn't find ticker")
		return true, err
	}

	newlog := sublog.With().Str("symbol", ticker.TickerSymbol).Logger()
	sublog = &newlog
	sublog.Info().Msg("got task to possibly update news for {symbol}")

	// skip calling API if we've succeeded at this recently
	lastdone := LastDone{Activity: "ticker_news", UniqueKey: ticker.TickerSymbol, LastStatus: "failed"}
	lastdone.getByActivity(db)
	if lastdone.LastStatus == "success" && lastdone.LastDoneDatetime.Valid && lastdone.LastDoneDatetime.Time.Add(minTickerNewsDelay*time.Minute).After(time.Now()) {
		sublog.Info().Str("last_retrieved", lastdone.LastDoneDatetime.Time.Format(sqlDateTime)).Msg("skipping {action} for {symbol}, recently received")
		return true, nil
	}

	// go get news
	sublog.Info().Msg("pulling news articles for {symbol}")
	err = loadMSNews(deps, ticker)
	if err == nil {
		lastdone.LastStatus = "success"
	} else {
		lastdone.LastStatus = fmt.Sprintf("%e", err)
	}
	lastdone.LastDoneDatetime = sql.NullTime{Valid: true, Time: time.Now()}

	err = lastdone.createOrUpdate(db)
	if err != nil {
		sublog.Error().Err(err).Msg("failed to create or update lastdone for {symbol}")
	}

	return true, nil
}

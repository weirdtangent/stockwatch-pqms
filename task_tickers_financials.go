package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	minTickerFinancialsDelay = 60
)

func perform_tickers_financials(deps *Dependencies, body *string) (bool, error) {
	db := deps.db
	sublog := deps.logger

	if body == nil || *body == "" {
		return false, fmt.Errorf("missing task body")
	}
	var taskTickerNewsBody TaskTickerNewsBody
	json.NewDecoder(strings.NewReader(*body)).Decode(&taskTickerNewsBody)

	if taskTickerNewsBody.TickerId == 0 && taskTickerNewsBody.TickerSymbol == "" {
		return false, fmt.Errorf("tickerId OR tickerSymbol must be provided")
	}

	ticker := Ticker{TickerId: taskTickerNewsBody.TickerId, TickerSymbol: taskTickerNewsBody.TickerSymbol}
	var err error
	if ticker.TickerId > 0 {
		err = ticker.getById(deps)
	} else {
		err = ticker.getBySymbol(deps)
	}
	if err != nil {
		return false, err
	}

	newlog := sublog.With().Str("symbol", ticker.TickerSymbol).Logger()
	sublog = &newlog

	lastdone := LastDone{Activity: "ticker_financials", UniqueKey: ticker.TickerSymbol, LastStatus: "failed"}
	_ = lastdone.getByActivity(db)

	if lastdone.LastStatus == "success" && lastdone.LastDoneDatetime.Valid && lastdone.LastDoneDatetime.Time.Add(minTickerFinancialsDelay*time.Minute).After(time.Now()) {
		sublog.Info().Str("last_retrieved", lastdone.LastDoneDatetime.Time.Format(sqlDateTime)).Msg("skipping {action} for {symbol}, recently received")
		return true, nil
	}

	// go get financials
	sublog.Info().Msg("pulling financials for {symbol}")
	err = loadBBfinancials(deps, ticker)
	if err != nil {
		lastdone.LastDoneDatetime = sql.NullTime{Valid: true, Time: time.Now()}
		lderr := lastdone.createOrUpdate(db)
		if lderr != nil {
			sublog.Error().Err(lderr).Msg("failed to create or update lastdone for {symbol}")
		}
		return true, err
	}

	// go get statistics
	sublog.Info().Msg("pulling statistics for {symbol}")
	err = loadBBstatistics(deps, ticker)
	if err == nil {
		lastdone.LastStatus = "success"
	}
	lastdone.LastDoneDatetime = sql.NullTime{Valid: true, Time: time.Now()}

	err = lastdone.createOrUpdate(db)
	if err != nil {
		sublog.Error().Err(err).Msg("failed to create or update lastdone for {symbol}")
	}

	return true, nil
}

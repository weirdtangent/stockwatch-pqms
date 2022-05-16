package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/weirdtangent/yhfinance"
)

type TaskTickerEODsBody struct {
	TickerId     uint64 `json:"ticker_id"`
	TickerSymbol string `json:"ticker_symbol"`
	ExchangeId   uint64 `json:"exchange_id"`
}

func perform_tickers_eods(deps *Dependencies, sublog zerolog.Logger, body *string) (bool, error) {
	db := deps.db

	if body == nil || *body == "" {
		sublog.Error().Msg("missing task body for {action}")
		return true, fmt.Errorf("missing task body")
	}
	var taskTickerEODsBody TaskTickerEODsBody
	json.NewDecoder(strings.NewReader(*body)).Decode(&taskTickerEODsBody)

	if taskTickerEODsBody.TickerId == 0 && taskTickerEODsBody.TickerSymbol == "" {
		sublog.Error().Msg("tickerId OR tickerSymbol must be provided")
		return true, fmt.Errorf("tickerId OR tickerSymbol must be provided")
	}

	ticker := Ticker{TickerId: taskTickerEODsBody.TickerId, TickerSymbol: taskTickerEODsBody.TickerSymbol}
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

	sublog = sublog.With().Str("symbol", ticker.TickerSymbol).Logger()
	sublog.Info().Msg("got task to possibly update news for {symbol}")

	// skip calling API if we've succeeded at this recently
	lastdone := LastDone{Activity: "ticker_eods", UniqueKey: ticker.TickerSymbol, LastStatus: "failed"}
	lastdone.getByActivity(db)
	if lastdone.LastStatus == "success" && lastdone.LastDoneDatetime.Valid && lastdone.LastDoneDatetime.Time.Add(minTickerEODsDelay*time.Minute).After(time.Now()) {
		sublog.Info().Str("last_retrieved", lastdone.LastDoneDatetime.Time.Format(sqlDateTime)).Msg("skipping {action} for {symbol}, recently received")
		return true, nil
	}

	// go get daily pricing from yhfinance
	sublog.Info().Msg("pulling daily pricing {symbol} from yhfinance")
	err = loadTickerEODsFromYH(deps, ticker)
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

// load ticker historical prices
func loadTickerEODsFromYH(deps *Dependencies, ticker Ticker) error {
	secrets := deps.secrets
	sublog := deps.logger

	historicalParams := map[string]string{"symbol": ticker.TickerSymbol}

	apiKey := secrets["yhfinance_rapidapi_key"]
	apiHost := secrets["yhfinance_rapidapi_host"]
	if apiKey == "" || apiHost == "" {
		sublog.Fatal().Msg("apiKey or apiHost secret is missing")
	}

	start := time.Now()
	response, err := yhfinance.GetFromYHFinance(sublog, apiKey, apiHost, "stockHistorical", historicalParams)
	sublog.Info().Int64("response_time", time.Since(start).Nanoseconds()).Msg("timer: yhfinance stockHistorical")
	if err != nil {
		sublog.Warn().Err(err).Str("ticker", ticker.TickerSymbol).Msg("failed to retrieve historical prices")
		return err
	}

	var historicalResponse yhfinance.YHHistoricalDataResponse
	json.NewDecoder(strings.NewReader(response)).Decode(&historicalResponse)

	var lastErr error
	for _, price := range historicalResponse.Prices {
		priceDatetime := time.Unix(price.Date, 0)
		tickerDaily := TickerDaily{0, "", ticker.TickerId, priceDatetime, price.Open, price.High, price.Low, price.Close, price.Volume, time.Now(), time.Now()}
		err = tickerDaily.createOrUpdate(deps)
		if err != nil {
			lastErr = err
		}
	}
	if lastErr != nil {
		log.Warn().Err(err).Str("ticker", ticker.TickerSymbol).Msg("failed to load at least one historical price")
	}

	for _, split := range historicalResponse.Events {
		tickerSplit := TickerSplit{0, "", ticker.TickerId, time.Unix(split.Date, 0), split.SplitRatio, time.Now(), time.Now()}
		err = tickerSplit.createIfNew(deps)
		if err != nil {
			lastErr = err
		}
	}
	if lastErr != nil {
		log.Warn().Err(err).Str("ticker", ticker.TickerSymbol).Msg("failed to load at least one historical split")
	}

	return nil
}

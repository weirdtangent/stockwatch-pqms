package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
)

type TaskTickerNewsBody struct {
	TickerId     uint64 `json:"ticker_id"`
	TickerSymbol string `json:"ticker_symbol"`
	ExchangeId   uint64 `json:"exchange_id"`
}

const (
	minTickerNewsDelay = 60
)

func perform_tickers_news(ctx context.Context, body *string) (bool, error) {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)

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
		err = ticker.getById(db)
	} else {
		err = ticker.getBySymbol(db)
	}
	if err != nil {
		return false, err
	}

	log := zerolog.Ctx(ctx).With().Str("symbol", ticker.TickerSymbol).Logger()
	ctx = log.WithContext(ctx)

	// skip calling API if we've succeeded at this recently
	lastdone := LastDone{Activity: "ticker_news", UniqueKey: ticker.TickerSymbol, LastStatus: "failed"}
	lastdone.getByActivity(db)
	if lastdone.LastStatus == "success" && lastdone.LastDoneDatetime.Valid && lastdone.LastDoneDatetime.Time.Add(minTickerNewsDelay*time.Minute).After(time.Now()) {
		zerolog.Ctx(ctx).Info().Str("last_retrieved", lastdone.LastDoneDatetime.Time.Format(sqlDateTime)).Msg("skipping {action} for {symbol}, recently received")
		return true, nil
	}

	// go get news
	log.Info().Msg("pulling news articles for {symbol}")
	err = loadMSNews(ctx, ticker)
	if err == nil {
		lastdone.LastStatus = "success"
	}
	lastdone.LastDoneDatetime = sql.NullTime{Valid: true, Time: time.Now()}

	err = lastdone.createOrUpdate(db)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to create or update lastdone")
	}

	return true, nil
}

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

const (
	minTickerFinancialsDelay = 60
)

func perform_tickers_financials(ctx context.Context, body *string) (bool, error) {
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
		err = ticker.getById(ctx)
	} else {
		err = ticker.getBySymbol(ctx)
	}
	if err != nil {
		return false, err
	}

	log := zerolog.Ctx(ctx).With().Str("symbol", ticker.TickerSymbol).Logger()
	ctx = log.WithContext(ctx)

	lastdone := LastDone{Activity: "ticker_financials", UniqueKey: ticker.TickerSymbol, LastStatus: "failed"}
	_ = lastdone.getByActivity(db)

	if lastdone.LastStatus == "success" && lastdone.LastDoneDatetime.Valid && lastdone.LastDoneDatetime.Time.Add(minTickerFinancialsDelay*time.Minute).After(time.Now()) {
		zerolog.Ctx(ctx).Info().Str("last_retrieved", lastdone.LastDoneDatetime.Time.Format(sqlDateTime)).Msg("skipping {action} for {symbol}, recently received")
		return true, nil
	}

	// go get financials
	zerolog.Ctx(ctx).Info().Msg("pulling financials for {symbol}")
	err = loadBBfinancials(ctx, ticker)
	if err != nil {
		lastdone.LastDoneDatetime = sql.NullTime{Valid: true, Time: time.Now()}
		lderr := lastdone.createOrUpdate(db)
		if lderr != nil {
			zerolog.Ctx(ctx).Error().Err(lderr).Msg("failed to create or update lastdone")
		}
		return true, err
	}

	// go get statistics
	zerolog.Ctx(ctx).Info().Msg("pulling statistics for {symbol}")
	err = loadBBstatistics(ctx, ticker)
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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
)

type TaskTickerNewsBody struct {
	TickerId     int64  `json:"ticker_id"`
	TickerSymbol string `json:"ticker_symbol"`
	ExchangeId   int64  `json:"exchange_id"`
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

	log.With().Str("ticker", ticker.TickerSymbol).Logger()

	lastdone := LastDone{Activity: "ticker_news", UniqueKey: ticker.TickerSymbol}
	_ = lastdone.getByActivity(db)

	if lastdone.LastDoneDatetime != "" {
		lastDoneTime, _ := time.Parse(sqlDateTime, lastdone.LastDoneDatetime)
		if lastDoneTime.After(time.Now().Add(minTickerNewsDelay * time.Minute)) {
			log.Info().Str("symbol", ticker.TickerSymbol).Str("last_retrieved", lastDoneTime.Format(sqlDateTime)).Msg("skipping news for {ticker}, {last_retrieved}")
			return true, nil
		}
	}

	// go get news
	log.Info().Str("symbol", ticker.TickerSymbol).Msg("pulling news articles for {symbol}")
	err = loadMSNews(ctx, ticker)
	if err != nil {
		lastdone.LastStatus = fmt.Sprintf("%s", err)
	} else {
		lastdone.LastStatus = "success"
	}
	lastdone.LastDoneDatetime = time.Now().Format(sqlDateTime)

	lastdone.createOrUpdate(db)

	return true, nil
}

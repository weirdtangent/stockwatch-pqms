package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
)

type TaskTickerNewsBody struct {
	TickerId     int64  `json:"ticker_id"`
	TickerSymbol string `json:"ticker_symbol"`
	ExchangeId   int64  `json:"exchange_id"`
}

const (
	minTickerNewsDelay = 1
)

func perform_tickers_news(ctx context.Context, body *string) (bool, error) {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)
	logger := ctx.Value(ContextKey("logger")).(zerolog.Logger)

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

	logger = logger.With().Str("ticker", ticker.TickerSymbol).Logger()
	ctx = context.WithValue(ctx, ContextKey("logger"), logger)

	lastdone := LastDone{Activity: "ticker_news", UniqueKey: ticker.TickerSymbol}
	_ = lastdone.getByActivity(db)

	if lastdone.LastDoneDatetime != "" {
		lastDoneTime, _ := time.Parse(sqlDateTime, lastdone.LastDoneDatetime)
		if lastDoneTime.After(time.Now().Add(minTickerNewsDelay * time.Minute)) {
			logger.Info().Msg(fmt.Sprintf("skipping ticker_news for %s, last retrieved %s", ticker.TickerSymbol, lastDoneTime.Format(sqlDateTime)))
			return true, nil
		}
	}

	// go get news
	logger.Info().Msg(fmt.Sprintf("pulling news articles for ticker %s", ticker.TickerSymbol))
	err = loadMSNews(ctx, ticker.TickerSymbol, ticker.TickerId)
	if err != nil {
		lastdone.LastStatus = fmt.Sprintf("%s", err)
	} else {
		lastdone.LastStatus = "success"
	}
	lastdone.LastDoneDatetime = time.Now().Format(sqlDateTime)

	lastdone.createOrUpdate(db)

	return true, nil
}

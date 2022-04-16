package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/jmoiron/sqlx"

	"github.com/rs/zerolog/log"

	"github.com/weirdtangent/marketstack"
	"github.com/weirdtangent/myaws"
)

type TickersEODTask struct {
	TaskAction string `json:"action"`
	TickerId   int64  `json:"ticker_id"`
	DaysBack   int    `json:"days_back"`
	Offset     int    `json:"offset"`
}

func perform_tickers_eod(ctx context.Context, body *string) (bool, error) {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)
	log := log.With().Str("queue", "tickers").Str("action", "eod").Logger()

	if body == nil || *body == "" {
		return false, fmt.Errorf("failed to get JSON body in task")
	}

	var EODTask TickersEODTask
	json.NewDecoder(strings.NewReader(*body)).Decode(&EODTask)

	if EODTask.TaskAction != "eod" {
		log.Error().Msg("Failed to decode JSON body in task or wrong taskAction")
		return false, fmt.Errorf("failed to decode JSON body in task or wrong taskAction")
	}

	ticker, err := getTickerById(db, EODTask.TickerId)
	if err != nil {
		log.Error().Msg("Failed to find ticker_id")
		return false, fmt.Errorf("failed to find ticker_id")
	}
	exchange, err := getExchangeById(db, ticker.ExchangeId)
	if err != nil {
		log.Error().Msg("Failed to load exchange")
		return false, fmt.Errorf("failed to find exchange")
	}

	log = log.With().Str("symbol", ticker.TickerSymbol).Str("acronym", exchange.ExchangeAcronym).Logger()

	err = fetchTickerEODs(ctx, ticker, exchange, EODTask.DaysBack, EODTask.Offset)
	if err != nil {
		log.Error().Err(err).Msg("Task failed, not retryable")
		return false, err
	}

	return true, nil
}

func fetchTickerEODs(ctx context.Context, ticker *Ticker, exchange *Exchange, days int, offset int) error {
	awssess := ctx.Value(ContextKey("awssess")).(*session.Session)
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)

	api_access_key, err := myaws.AWSGetSecretKV(awssess, "marketstack", "api_access_key")
	if err != nil {
		log.Error().Err(err).Msg("Failed to get marketstack API key, can retry")
		return nil
	}

	EOD, err := marketstack.FetchTickerEOD(*api_access_key, ticker.TickerSymbol, exchange.ExchangeMic, days, offset)
	if err != nil {
		return err
	}

	var anyErr error
	for _, MSIndexData := range EOD.EndOfDay {
		var ticker_daily = &TickerDaily{0, ticker.TickerId, MSIndexData.PriceDate, MSIndexData.OpenPrice, MSIndexData.HighPrice, MSIndexData.LowPrice, MSIndexData.ClosePrice, MSIndexData.Volume, "", ""}
		if ticker_daily.Volume > 0 {
			_, err = createOrUpdateTickerDaily(db, ticker_daily)
			if err != nil {
				anyErr = err
			}
		} else {
			log.Warn().
				Str("symbol", ticker.TickerSymbol).
				Int64("ticker_id", ticker.TickerId).
				Str("price_date", MSIndexData.PriceDate).
				Msg("Failed to get any volume for today")
		}
	}
	if anyErr != nil {
		log.Error().Err(anyErr).
			Str("symbol", ticker.TickerSymbol).
			Int64("ticker_id", ticker.TickerId).
			Msg("Failed to create/update 1 or more EOD for ticker")
	}

	return nil
}

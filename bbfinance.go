package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
	"github.com/weirdtangent/bbfinance"
)

type Financials struct {
	FinancialsId   uint64       `json:"financials_id"`
	TickerId       uint64       `db:"ticker_id"`
	FormName       string       `db:"form_name"`
	FormTermName   string       `db:"form_term_name"`
	ChartName      string       `db:"chart_name"`
	ChartDatetime  sql.NullTime `db:"chart_datetime"`
	ChartType      string       `db:"chart_type"`
	IsPercentage   bool         `db:"is_percentage"`
	ChartValue     float64      `db:"chart_value"`
	CreateDatetime sql.NullTime `db:"create_datetime"`
	UpdateDatetime sql.NullTime `db:"update_datetime"`
}

func (f *Financials) createOrUpdate(ctx context.Context) error {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)

	var insertOrUpdate = "INSERT INTO financials SET ticker_id=?, form_name=?, form_term_name=?, chart_name=?, chart_datetime=?, chart_type=?, is_percentage=?, chart_value=?, create_datetime=now() ON DUPLICATE KEY UPDATE chart_value=?, update_datetime=now()"

	_, err := db.Exec(insertOrUpdate, f.TickerId, f.FormName, f.FormTermName, f.ChartName, f.ChartDatetime, f.ChartType, f.IsPercentage, f.ChartValue, f.ChartValue)
	if err != nil {
		zerolog.Ctx(ctx).Fatal().Err(err).
			Str("table_name", "financials").
			Msg("Failed on INSERT OR UPDATE")
		return err
	}
	return nil
}

var (
	chart_date_format = map[string]string{
		"Annual":    "2006",
		"Quarterly": "1/2006",
	}
)

func loadBBfinancials(ctx context.Context, ticker Ticker) error {
	apiKey := ctx.Value(ContextKey("bbfinance_apikey")).(string)
	apiHost := ctx.Value(ContextKey("bbfinance_apihost")).(string)

	var err error

	autoCompleteResponse := bbfinance.BBAutoCompleteResponse{}
	autoCompleteResponse, err = bbfinance.BBAutoComplete(ctx, apiKey, apiHost, ticker.TickerSymbol)
	if err != nil {
		return err
	}

	for _, result := range autoCompleteResponse.Results {
		if result.Symbol != ticker.TickerSymbol || result.Currency != "USD" {
			continue
		}
		id := result.Id
		financialsResponse, err := bbfinance.BBGetFinancials(ctx, apiKey, apiHost, id)
		if err != nil || len(financialsResponse.Results) == 0 {
			zerolog.Ctx(ctx).Error().Err(err).Str("id", id).Msg("failed to get financials from {id}")
			return err
		}
		zerolog.Ctx(ctx).Info().Msg("pulling financials for {symbol}")
		for _, financialResult := range financialsResponse.Results {
			resultName := financialResult.Name // "Income Statement", "Balance Sheet", "Cash Flow"
			for _, financialSheet := range financialResult.TimeBasedSheets {
				sheetName := financialSheet.Name // quarterly, annual, etc
				dateFormat, ok := chart_date_format[sheetName]
				if !ok {
					zerolog.Ctx(ctx).Error().Err(fmt.Errorf("invalid sheetName '%s'", sheetName)).Msg("could not load financials")
					return fmt.Errorf("invalid sheetName")
				}
				for _, financialChartData := range financialSheet.ChartData {
					chartName := financialChartData.Name      // "Revenue", "Net Income", "Profit Margin", etc
					chartType := financialChartData.ChartType // bar, line, etc
					isPercentage := financialChartData.IsPercentage
					for colKey, colData := range financialChartData.Values {
						datetime, err := time.Parse(dateFormat, financialSheet.ColHeadings[colKey])
						chartDatetime := sql.NullTime{Valid: err == nil, Time: datetime}
						financials := Financials{0, ticker.TickerId, resultName, sheetName, chartName, chartDatetime, chartType, isPercentage, colData, sql.NullTime{}, sql.NullTime{}}
						err = financials.createOrUpdate(ctx)
						if err != nil {
							zerolog.Ctx(ctx).Error().Err(err).Msg("failed to create/update financial data")
						}
					}
				}

			}
		}
	}
	return nil
}

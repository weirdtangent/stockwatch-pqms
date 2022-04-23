package main

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
	"github.com/weirdtangent/bbfinance"
)

type Financials struct {
	FinancialsId    int64   `json:"financials_id"`
	TickerId        int64   `db:"ticker_id"`
	FormName        string  `db:"form_name"`
	FormTermName    string  `db:"form_term_name"`
	ChartName       string  `db:"chart_name"`
	ChartDateString string  `db:"chart_date_string"`
	ChartType       string  `db:"chart_type"`
	IsPercentage    bool    `db:"is_percentage"`
	ChartValue      float64 `db:"chart_value"`
	CreateDatetime  string  `db:"create_datetime"`
	UpdateDatetime  string  `db:"update_datetime"`
}

func (f *Financials) createOrUpdate(ctx context.Context) error {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)

	var insertOrUpdate = "INSERT INTO financials SET ticker_id=?, form_name=?, form_term_name=?, chart_name=?, chart_date_string=?, chart_type=?, is_percentage=?, chart_value=?, create_datetime=now() ON DUPLICATE KEY UPDATE chart_value=?, update_datetime=now()"

	_, err := db.Exec(insertOrUpdate, f.TickerId, f.FormName, f.FormTermName, f.ChartName, f.ChartDateString, f.ChartType, f.IsPercentage, f.ChartValue, f.ChartValue)
	if err != nil {
		log.Fatal().Err(err).
			Str("table_name", "financials").
			Msg("Failed on INSERT OR UPDATE")
		return err
	}
	return nil
}

func loadBBfinancials(ctx context.Context, ticker Ticker) error {
	apiKey := ctx.Value(ContextKey("bbfinance_apikey")).(string)
	apiHost := ctx.Value(ContextKey("bbfinance_apihost")).(string)

	var err error

	autoCompleteResponse := bbfinance.BBAutoCompleteResponse{}
	autoCompleteResponse, err = bbfinance.BBAutoComplete(apiKey, apiHost, ticker.TickerSymbol)
	if err != nil {
		return err
	}

	for _, result := range autoCompleteResponse.Results {
		log := log.With().Str("symbol", result.Symbol).Logger()
		if result.Symbol != ticker.TickerSymbol || result.Currency != "USD" {
			continue
		}
		id := result.Id
		financialsResponse, err := bbfinance.BBGetFinancials(apiKey, apiHost, id)
		if err != nil || len(financialsResponse.Results) == 0 {
			log.Error().Err(err).Str("id", id).Msg("failed to get financials from {id}")
			return err
		}
		log.Info().Msg("pulling financials for {symbol}")
		for _, financialResult := range financialsResponse.Results {
			resultName := financialResult.Name // "Income Statement", "Balance Sheet", "Cash Flow"
			log := log.With().Str("form_name", resultName).Logger()
			for _, financialSheet := range financialResult.TimeBasedSheets {
				sheetName := financialSheet.Name // quarterly, annual, etc
				log := log.With().Str("term_name", sheetName).Logger()
				for _, financialChartData := range financialSheet.ChartData {
					chartName := financialChartData.Name // "Revenue", "Net Income", "Profit Margin", etc
					log := log.With().Str("chart_name", chartName).Logger()
					chartType := financialChartData.ChartType // bar, line, etc
					isPercentage := financialChartData.IsPercentage
					for colKey, colData := range financialChartData.Values {
						financials := Financials{0, ticker.TickerId, resultName, sheetName, chartName, financialSheet.ColHeadings[colKey], chartType, isPercentage, colData, "", ""}
						err := financials.createOrUpdate(ctx)
						if err != nil {
							log.Error().Err(err).Msg("failed to create/update financial data")
						}
					}
				}

			}
		}
	}
	return nil
}

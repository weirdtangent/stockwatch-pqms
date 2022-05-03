package main

import (
	"fmt"
)

type TickersEODTask struct {
	TaskAction string `json:"action"`
	TickerId   uint64 `json:"ticker_id"`
	DaysBack   int    `json:"days_back"`
	Offset     int    `json:"offset"`
}

func perform_tickers_eod(deps *Dependencies, body *string) (bool, error) {
	if body != nil {
		return false, fmt.Errorf("just testing")
	}
	return false, fmt.Errorf("just testing")
}

package main

import (
	"fmt"

	"github.com/rs/zerolog"
)

func perform_tickers_intraday(deps *Dependencies, sublog zerolog.Logger, body *string) (bool, error) {
	if body != nil {
		return false, fmt.Errorf("just testing")
	}
	return false, fmt.Errorf("just testing")
}

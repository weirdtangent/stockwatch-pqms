package main

import (
	"fmt"
)

func perform_tickers_intraday(deps *Dependencies, body *string) (bool, error) {
	if body != nil {
		return false, fmt.Errorf("just testing")
	}
	return false, fmt.Errorf("just testing")
}

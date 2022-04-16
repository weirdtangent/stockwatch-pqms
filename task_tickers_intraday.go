package main

import (
	"context"
	"fmt"
)

func perform_tickers_intraday(ctx context.Context, body *string) (bool, error) {
	if body != nil {
		return false, fmt.Errorf("just testing")
	}
	return false, fmt.Errorf("just testing")
}

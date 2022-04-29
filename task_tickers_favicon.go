package main

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
	"golang.org/x/net/html"
)

type TaskTickerFavIconBody struct {
	TickerId     uint64 `json:"ticker_id"`
	TickerSymbol string `json:"ticker_symbol"`
	ExchangeId   uint64 `json:"exchange_id"`
}

const (
	minTickerFavIconDelay = 60 * 24 * 30 // 30 days
)

func perform_tickers_favicon(ctx context.Context, body *string) (bool, error) {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)

	if body == nil || *body == "" {
		zerolog.Ctx(ctx).Error().Msg("missing task body")
		return true, fmt.Errorf("missing task body")
	}
	var taskTickerFavIconBody TaskTickerFavIconBody
	json.NewDecoder(strings.NewReader(*body)).Decode(&taskTickerFavIconBody)

	if taskTickerFavIconBody.TickerId == 0 && taskTickerFavIconBody.TickerSymbol == "" {
		zerolog.Ctx(ctx).Error().Msg("tickerId OR tickerSymbol must be provided")
		return true, fmt.Errorf("tickerId OR tickerSymbol must be provided")
	}

	ticker := Ticker{TickerId: taskTickerFavIconBody.TickerId, TickerSymbol: taskTickerFavIconBody.TickerSymbol}
	var err error
	if ticker.TickerId > 0 {
		err = ticker.getById(ctx)
	} else {
		err = ticker.getBySymbol(ctx)
	}
	if err != nil {
		zerolog.Ctx(ctx).Error().Interface("ticker", ticker).Msg("couldn't find ticker")
		return true, err
	}

	zerolog.Ctx(ctx).Info().Msg("got task to possibly update favicon for {symbol}")

	// skip calling API if we've succeeded at this recently
	lastdone := LastDone{Activity: "ticker_favicon", UniqueKey: ticker.TickerSymbol, LastStatus: "failed"}
	lastdone.getByActivity(db)
	if lastdone.LastStatus == "success" && lastdone.LastDoneDatetime.Valid && lastdone.LastDoneDatetime.Time.Add(minTickerFavIconDelay*time.Minute).After(time.Now()) {
		zerolog.Ctx(ctx).Info().Str("last_retrieved", lastdone.LastDoneDatetime.Time.Format(sqlDateTime)).Msg("skipping {action} for {symbol}, recently received")
		return true, nil
	}

	// go get favicon
	err = saveFavIcon(ctx, ticker)

	return true, err
}

func saveFavIcon(ctx context.Context, ticker Ticker) error {
	awssess := ctx.Value(ContextKey("awssess")).(*session.Session)
	iconUrl := ""

	if ticker.Website == "" {
		ticker.FavIconS3Key = "none"
		ticker.createOrUpdate(ctx)
		return fmt.Errorf("website not defined for symbol")
	}

	// go see if website indicates iconUrl
	resp, err := http.Get(ticker.Website)
	if err == nil {
		parser := html.NewTokenizer(resp.Body)
		for {
			nextTag := parser.Next()
			if nextTag == html.ErrorToken {
				break
			}
			if nextTag != html.StartTagToken && nextTag != html.SelfClosingTagToken {
				continue
			}
			tagName, hasAttr := parser.TagName()
			if string(tagName) != "link" || !hasAttr {
				continue
			}

			rightTag := false
			attrs := make(map[string]string)
			for {
				attrKey, attrVal, moreAttr := parser.TagAttr()
				attrs[string(attrKey)] = string(attrVal)
				if !moreAttr {
					break
				}
			}
			val, ok := attrs["rel"]
			rightTag = ok && (val == "icon" || val == "shortcut icon")
			val, ok = attrs["href"]
			if ok {
				iconUrl = val
			}
			if rightTag && iconUrl != "" {
				break
			}
			rightTag = false
			iconUrl = ""
		}
	} else {
		zerolog.Ctx(ctx).Error().Err(err).Str("url", ticker.Website).Msg("failed to get website page")
	}

	if iconUrl != "" {
		if absoluteUrl.MatchString(iconUrl) {
			// we're good
		} else if relativeProtocolUrl.MatchString(iconUrl) {
			protocol := getProtocolUrl.FindString(ticker.Website)
			iconUrl = protocol + iconUrl
		} else if relativePathUrl.MatchString(iconUrl) {
			path := relativePathUrl.FindString(iconUrl)
			iconUrl = ticker.Website + path
		} else {
			iconUrl = ticker.Website + "/" + iconUrl
		}
	} else {
		iconUrl = ticker.Website + "/favicon.ico"
	}
	zerolog.Ctx(ctx).Info().Str("url", iconUrl).Msg("getting favicon.ico from {url}")

	resp, err = http.Get(iconUrl)
	if err != nil {
		ticker.FavIconS3Key = "none"
		ticker.createOrUpdate(ctx)
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	faviconData := string(body)

	s3svc := s3.New(awssess)
	sha1Hash := sha1.New()
	io.WriteString(sha1Hash, faviconData)
	s3Key := fmt.Sprintf("Tickers/FavIcons/%s-%x", ticker.TickerSymbol, sha1Hash.Sum(nil))

	inputPutObj := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader(faviconData)),
		Bucket: aws.String(awsPrivateBucketName),
		Key:    aws.String(s3Key),
	}

	_, err = s3svc.PutObject(inputPutObj)
	if err != nil {
		return err
	}
	ticker.FavIconS3Key = s3Key
	ticker.createOrUpdate(ctx)

	return nil
}

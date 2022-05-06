package main

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/rs/zerolog"
	"golang.org/x/net/html"
)

type TaskTickerFavIconBody struct {
	TickerId     uint64 `json:"ticker_id"`
	TickerSymbol string `json:"ticker_symbol"`
	ExchangeId   uint64 `json:"exchange_id"`
}

func perform_tickers_favicon(deps *Dependencies, sublog zerolog.Logger, body *string) (bool, error) {
	db := deps.db

	if body == nil || *body == "" {
		sublog.Error().Msg("missing task body")
		return true, fmt.Errorf("missing task body")
	}
	var taskTickerFavIconBody TaskTickerFavIconBody
	json.NewDecoder(strings.NewReader(*body)).Decode(&taskTickerFavIconBody)

	if taskTickerFavIconBody.TickerId == 0 && taskTickerFavIconBody.TickerSymbol == "" {
		sublog.Error().Msg("tickerId OR tickerSymbol must be provided")
		return true, fmt.Errorf("tickerId OR tickerSymbol must be provided")
	}

	ticker := Ticker{TickerId: taskTickerFavIconBody.TickerId, TickerSymbol: taskTickerFavIconBody.TickerSymbol}
	var err error
	if ticker.TickerId > 0 {
		err = ticker.getById(deps)
	} else {
		err = ticker.getBySymbol(deps)
	}
	if err != nil {
		sublog.Error().Interface("ticker", ticker).Msg("couldn't find ticker")
		return true, err
	}

	sublog = sublog.With().Str("symbol", ticker.TickerSymbol).Logger()
	sublog.Info().Msg("got task to possibly update favicon for {symbol}")

	// skip calling API if we've succeeded at this recently
	lastdone := LastDone{Activity: "ticker_favicon", UniqueKey: ticker.TickerSymbol, LastStatus: "failed"}
	lastdone.getByActivity(db)
	if lastdone.LastStatus == "success" && lastdone.LastDoneDatetime.Valid && lastdone.LastDoneDatetime.Time.Add(minTickerFavIconDelay*time.Minute).After(time.Now()) {
		sublog.Info().Str("last_retrieved", lastdone.LastDoneDatetime.Time.Format(sqlDateTime)).Msg("skipping {action} for {symbol}, recently received")
		return true, nil
	}

	// go get favicon
	err = saveFavIcon(deps, sublog, ticker)

	return true, err
}

func saveFavIcon(deps *Dependencies, sublog zerolog.Logger, ticker Ticker) error {
	awssess := deps.awssess

	iconUrl := ""

	if ticker.Website == "" {
		ticker.FavIconS3Key = "none"
		ticker.createOrUpdate(deps)
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
		sublog.Error().Err(err).Str("url", ticker.Website).Msg("failed to get website page")
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
	sublog.Info().Str("url", iconUrl).Msg("getting favicon.ico from {url}")

	resp, err = http.Get(iconUrl)
	if err != nil || resp.StatusCode != http.StatusOK {
		ticker.FavIconS3Key = "none"
		ticker.createOrUpdate(deps)
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
	ticker.createOrUpdate(deps)

	return nil
}

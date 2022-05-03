package main

import (
	"database/sql"
	"errors"
	"regexp"
	"time"

	"github.com/weirdtangent/msfinance"
)

type Article struct {
	ArticleId          uint64       `db:"article_id"`
	SourceId           uint64       `db:"source_id"`
	ExternalId         string       `db:"external_id"`
	PublishedDatetime  sql.NullTime `db:"published_datetime"`
	PubUpdatedDatetime sql.NullTime `db:"pubupdated_datetime"`
	Title              string       `db:"title"`
	Body               string       `db:"body"`
	ArticleURL         string       `db:"article_url"`
	ImageURL           string       `db:"image_url"`
	CreateDatetime     sql.NullTime `db:"create_datetime"`
	UpdateDatetime     sql.NullTime `db:"update_datetime"`
}

type ArticleTicker struct {
	ArticleTickerId uint64       `db:"article_ticker_id"`
	ArticleId       uint64       `db:"article_id"`
	TickerSymbol    string       `db:"ticker_symbol"`
	TickerId        uint64       `db:"ticker_id"`
	CreateDatetime  sql.NullTime `db:"create_datetime"`
	UpdateDatetime  sql.NullTime `db:"update_datetime"`
}

func loadMSNews(deps *Dependencies, ticker Ticker) error {
	secrets := deps.secrets
	sublog := deps.logger

	apiKey := secrets["msfinance_rapidapi_key"]
	apiHost := secrets["msfinance_rapidapi_host"]

	var err error

	autoCompleteResponse := msfinance.MSAutoCompleteResponse{}
	if ticker.MSPerformanceId == "" {
		autoCompleteResponse, err = msfinance.MSAutoComplete(sublog, apiKey, apiHost, ticker.TickerSymbol)
		if err != nil {
			return err
		}
	} else {
		autoCompleteResponse.Results = append(
			autoCompleteResponse.Results,
			msfinance.MSAutoCompleteResult{
				Symbol:        ticker.TickerSymbol,
				PerformanceId: ticker.MSPerformanceId,
			})
	}

	performanceIds := make(map[string]bool)
	for _, result := range autoCompleteResponse.Results {
		performanceId := result.PerformanceId
		if ticker.TickerSymbol == result.Symbol {
			updateTickerPerformanceId(deps, ticker.TickerId, performanceId)
		}
		if _, ok := performanceIds[performanceId]; !ok {
			performanceIds[performanceId] = true

			newsListResponse, err := msfinance.MSGetNewsList(sublog, apiKey, apiHost, performanceId)
			if err != nil {
				return err
			}

			for _, story := range newsListResponse {
				sourceId, err := getSourceId(deps, story.SourceId)
				if err != nil {
					sublog.Error().Err(err).Msg("unknown source, skipping news article")
					continue
				}

				if existingId, err := getArticleByExternalId(deps, sourceId, story.InternalId); err != nil {
					sublog.Info().Err(err).Str("existing_id", story.InternalId)
				} else if existingId != 0 {
					continue
				} else {
					content, err := getNewsItemContent(deps, story.SourceId, story.InternalId)
					if err != nil || len(content) == 0 {
						sublog.Error().Err(err).Msg("no news item content found")
						continue
					}

					publishedDateTime, err := time.Parse("2006-01-02T15:04:05-07:00", story.Published)
					if err != nil {
						sublog.Error().Err(err).Msg("could not parse Published")
						continue
					}

					article := Article{0, sourceId, story.InternalId, sql.NullTime{Valid: true, Time: publishedDateTime}, sql.NullTime{Valid: true, Time: publishedDateTime}, story.Title, content, "", "", sql.NullTime{}, sql.NullTime{}}

					err = article.createArticle(deps)
					if err != nil {
						sublog.Warn().Err(err).Str("symbol", ticker.TickerSymbol).Msg("failed to write new news article")
					}

					articleTicker := ArticleTicker{0, article.ArticleId, ticker.TickerSymbol, ticker.TickerId, sql.NullTime{}, sql.NullTime{}}
					err = articleTicker.createArticleTicker(deps)
					if err != nil {
						sublog.Warn().Err(err).Str("symbol", ticker.TickerSymbol).Msg("failed to write ticker(s) for new article")
					}
				}
			}
		}
	}
	return nil
}

func getNewsItemContent(deps *Dependencies, sourceId string, internalId string) (string, error) {
	secrets := deps.secrets
	sublog := deps.logger

	apiKey := secrets["msfinance_rapidapi_key"]
	apiHost := secrets["msfinance_rapidapi_host"]

	newsDetailsResponse, err := msfinance.MSGetNewsDetails(sublog, apiKey, apiHost, internalId, sourceId)
	if err != nil {
		return "", err
	}

	newsContent := followContent(newsDetailsResponse.ContentObj)
	sublog.Info().Int("bytes", len(newsContent)).Msg("found content of {bytes} for article")
	return newsContent, nil
}

func followContent(contentObj []msfinance.MSNewsContentObj) string {
	noSpaces := regexp.MustCompile(`^\S+$`)

	var content string
	for _, contentPiece := range contentObj {
		var deeperContent string
		if len(contentPiece.ContentObj) > 0 {
			deeperContent = followContent(contentPiece.ContentObj)
		} else {
			deeperContent = contentPiece.Content
		}
		switch contentPiece.Type {
		case "text":
			content += deeperContent
		case "img":
			content += `<img src="` + contentPiece.Src + `">`
		case "a":
			if noSpaces.MatchString(deeperContent) {
				content += `<a href="` + deeperContent + `">` + deeperContent + `</a>`
			} else {
				content += deeperContent
			}
		default:
			content += `<` + contentPiece.Type + `>` + deeperContent + `</` + contentPiece.Type + `>`
		}
	}

	return content
}

func getSourceId(deps *Dependencies, source string) (uint64, error) {
	db := deps.db

	var sourceId uint64
	err := db.QueryRowx("SELECT source_id FROM source WHERE source_string=?", source).Scan(&sourceId)
	return sourceId, err
}

func (a *Article) getArticleById(deps *Dependencies) error {
	db := deps.db

	err := db.QueryRowx("SELECT * FROM article WHERE article_id=?", a.ArticleId).StructScan(a)
	return err
}

func getArticleByExternalId(deps *Dependencies, sourceId uint64, externalId string) (uint64, error) {
	db := deps.db
	sublog := deps.logger

	var articleId uint64
	err := db.QueryRowx("SELECT article_id FROM article WHERE source_id=? && external_id=?", sourceId, externalId).Scan(&articleId)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		} else {
			sublog.Warn().Err(err).Str("table_name", "article").Msg("Failed to check for existing record")
		}
	}
	return articleId, err
}

func (a *Article) createArticle(deps *Dependencies) error {
	db := deps.db
	sublog := deps.logger

	var insert = "INSERT INTO article SET source_id=?, external_id=?, published_datetime=?, pubupdated_datetime=?, title=?, body=?, article_url=?, image_url=?"

	res, err := db.Exec(insert, a.SourceId, a.ExternalId, a.PublishedDatetime, a.PubUpdatedDatetime, a.Title, a.Body, a.ArticleURL, a.ImageURL)
	if err != nil {
		sublog.Fatal().Err(err).Str("table_name", "article").Msg("failed on INSERT")
	}
	recordId, err := res.LastInsertId()
	if err != nil || recordId == 0 {
		sublog.Fatal().Err(err).Str("table_name", "article").Msg("failed on LAST_INSERT_ID")
	}
	a.ArticleId = uint64(recordId)
	return a.getArticleById(deps)
}

func (at *ArticleTicker) getArticleTickerById(deps *Dependencies) error {
	db := deps.db

	err := db.QueryRowx("SELECT * FROM article_ticker WHERE article_ticker_id=?", at.ArticleTickerId).StructScan(at)
	return err
}

func (at *ArticleTicker) createArticleTicker(deps *Dependencies) error {
	db := deps.db
	sublog := deps.logger

	var insert = "INSERT INTO article_ticker SET article_id=?, ticker_symbol=?, ticker_id=?"

	res, err := db.Exec(insert, at.ArticleId, at.TickerSymbol, at.TickerId)
	if err != nil {
		sublog.Fatal().Err(err).
			Str("table_name", "article_ticker").
			Msg("Failed on INSERT")
	}
	recordId, err := res.LastInsertId()
	if err != nil || recordId == 0 {
		sublog.Fatal().Err(err).
			Str("table_name", "article_ticker").
			Msg("Failed on LAST_INSERT_ID")
	}
	at.ArticleTickerId = uint64(recordId)
	return at.getArticleTickerById(deps)
}

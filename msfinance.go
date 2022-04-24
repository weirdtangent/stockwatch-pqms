package main

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
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

func loadMSNews(ctx context.Context, ticker Ticker) error {
	apiKey := ctx.Value(ContextKey("msfinance_apikey")).(string)
	apiHost := ctx.Value(ContextKey("msfinance_apihost")).(string)

	var err error

	autoCompleteResponse := msfinance.MSAutoCompleteResponse{}
	if ticker.MSPerformanceId == "" {
		autoCompleteResponse, err = msfinance.MSAutoComplete(ctx, apiKey, apiHost, ticker.TickerSymbol)
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
			updateTickerById(ctx, ticker.TickerId, performanceId)
		}
		if _, ok := performanceIds[performanceId]; !ok {
			performanceIds[performanceId] = true

			newsListResponse, err := msfinance.MSGetNewsList(ctx, apiKey, apiHost, performanceId)
			if err != nil {
				return err
			}

			for _, story := range newsListResponse {
				sourceId, err := getSourceId(ctx, story.SourceId)
				if err != nil {
					zerolog.Ctx(ctx).Error().Err(err).Msg("unknown source, skipping news article")
					continue
				}

				if existingId, err := getArticleByExternalId(ctx, sourceId, story.InternalId); err != nil {
					zerolog.Ctx(ctx).Info().Err(err).Str("existing_id", story.InternalId)
				} else if existingId != 0 {
					continue
				} else {
					content, err := getNewsItemContent(ctx, story.SourceId, story.InternalId)
					if err != nil || len(content) == 0 {
						zerolog.Ctx(ctx).Error().Err(err).Msg("no news item content found")
						continue
					}

					publishedDateTime, err := time.Parse("2006-01-02T15:04:05-07:00", story.Published)
					if err != nil {
						zerolog.Ctx(ctx).Error().Err(err).Msg("could not parse Published")
						continue
					}

					article := Article{0, sourceId, story.InternalId, sql.NullTime{Valid: true, Time: publishedDateTime}, sql.NullTime{Valid: true, Time: publishedDateTime}, story.Title, content, "", "", sql.NullTime{}, sql.NullTime{}}

					err = article.createArticle(ctx)
					if err != nil {
						zerolog.Ctx(ctx).Warn().Err(err).Str("symbol", ticker.TickerSymbol).Msg("failed to write new news article")
					}

					articleTicker := ArticleTicker{0, article.ArticleId, ticker.TickerSymbol, ticker.TickerId, sql.NullTime{}, sql.NullTime{}}
					err = articleTicker.createArticleTicker(ctx)
					if err != nil {
						zerolog.Ctx(ctx).Warn().Err(err).Str("symbol", ticker.TickerSymbol).Msg("failed to write ticker(s) for new article")
					}
				}
			}
		}
	}
	return nil
}

func getNewsItemContent(ctx context.Context, sourceId string, internalId string) (string, error) {
	apiKey := ctx.Value(ContextKey("msfinance_apikey")).(string)
	apiHost := ctx.Value(ContextKey("msfinance_apihost")).(string)

	newsDetailsResponse, err := msfinance.MSGetNewsDetails(ctx, apiKey, apiHost, internalId, sourceId)
	if err != nil {
		return "", err
	}

	newsContent := followContent(newsDetailsResponse.ContentObj)
	zerolog.Ctx(ctx).Info().Int("bytes", len(newsContent)).Msg("found content of {bytes} for article")
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

func getSourceId(ctx context.Context, source string) (uint64, error) {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)

	var sourceId uint64
	err := db.QueryRowx("SELECT source_id FROM source WHERE source_string=?", source).Scan(&sourceId)
	return sourceId, err
}

func (a *Article) getArticleById(ctx context.Context) error {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)

	err := db.QueryRowx("SELECT * FROM article WHERE article_id=?", a.ArticleId).StructScan(a)
	return err
}

func getArticleByExternalId(ctx context.Context, sourceId uint64, externalId string) (uint64, error) {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)

	var articleId uint64
	err := db.QueryRowx("SELECT article_id FROM article WHERE source_id=? && external_id=?", sourceId, externalId).Scan(&articleId)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		} else {
			zerolog.Ctx(ctx).Warn().Err(err).Str("table_name", "article").Msg("Failed to check for existing record")
		}
	}
	return articleId, err
}

func (a *Article) createArticle(ctx context.Context) error {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)

	var insert = "INSERT INTO article SET source_id=?, external_id=?, published_datetime=?, pubupdated_datetime=?, title=?, body=?, article_url=?, image_url=?"

	res, err := db.Exec(insert, a.SourceId, a.ExternalId, a.PublishedDatetime, a.PubUpdatedDatetime, a.Title, a.Body, a.ArticleURL, a.ImageURL)
	if err != nil {
		zerolog.Ctx(ctx).Fatal().Err(err).
			Str("table_name", "article").
			Msg("Failed on INSERT")
	}
	recordId, err := res.LastInsertId()
	if err != nil || recordId == 0 {
		zerolog.Ctx(ctx).Fatal().Err(err).
			Str("table_name", "article").
			Msg("Failed on LAST_INSERT_ID")
	}
	a.ArticleId = uint64(recordId)
	return a.getArticleById(ctx)
}

func (at *ArticleTicker) getArticleTickerById(ctx context.Context) error {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)

	err := db.QueryRowx("SELECT * FROM article_ticker WHERE article_ticker_id=?", at.ArticleTickerId).StructScan(at)
	return err
}

func (at *ArticleTicker) createArticleTicker(ctx context.Context) error {
	db := ctx.Value(ContextKey("db")).(*sqlx.DB)

	var insert = "INSERT INTO article_ticker SET article_id=?, ticker_symbol=?, ticker_id=?"

	res, err := db.Exec(insert, at.ArticleId, at.TickerSymbol, at.TickerId)
	if err != nil {
		zerolog.Ctx(ctx).Fatal().Err(err).
			Str("table_name", "article_ticker").
			Msg("Failed on INSERT")
	}
	recordId, err := res.LastInsertId()
	if err != nil || recordId == 0 {
		zerolog.Ctx(ctx).Fatal().Err(err).
			Str("table_name", "article_ticker").
			Msg("Failed on LAST_INSERT_ID")
	}
	at.ArticleTickerId = uint64(recordId)
	return at.getArticleTickerById(ctx)
}

package main

import (
	"database/sql"
	"errors"
	"time"
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
	CreateDatetime     time.Time    `db:"create_datetime"`
	UpdateDatetime     time.Time    `db:"update_datetime"`
}

type ArticleTicker struct {
	ArticleTickerId uint64    `db:"article_ticker_id"`
	ArticleId       uint64    `db:"article_id"`
	TickerSymbol    string    `db:"ticker_symbol"`
	TickerId        uint64    `db:"ticker_id"`
	CreateDatetime  time.Time `db:"create_datetime"`
	UpdateDatetime  time.Time `db:"update_datetime"`
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

func getArticleByExternalId(deps *Dependencies, externalId string) (uint64, error) {
	db := deps.db
	sublog := deps.logger

	var articleId uint64
	err := db.QueryRowx("SELECT article_id FROM article WHERE external_id=?", externalId).Scan(&articleId)
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

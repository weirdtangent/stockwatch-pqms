package main

import (
	"database/sql"
	"regexp"
	"time"

	"github.com/weirdtangent/msfinance"
)

func loadMSNews(deps *Dependencies, ticker Ticker) error {
	secrets := deps.secrets
	sublog := deps.logger.With().Str("symbol", ticker.TickerSymbol).Logger()

	apiKey := secrets["msfinance_rapidapi_key"]
	apiHost := secrets["msfinance_rapidapi_host"]

	var err error

	autoCompleteResponse := msfinance.MSAutoCompleteResponse{}
	if ticker.MSPerformanceId == "" {
		autoCompleteResponse, err = msfinance.MSAutoComplete(&sublog, apiKey, apiHost, ticker.TickerSymbol)
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

			newsListResponse, err := msfinance.MSGetNewsList(&sublog, apiKey, apiHost, performanceId)
			if err != nil {
				return err
			}

			for _, story := range newsListResponse {
				sourceId, err := getSourceId(deps, story.SourceId)
				if err != nil {
					sublog.Error().Err(err).Msg("unknown source, skipping news article")
					continue
				}

				if existingId, err := getArticleByExternalId(deps, story.InternalId); err != nil {
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

					article := Article{0, sourceId, story.InternalId, sql.NullTime{Valid: true, Time: publishedDateTime}, sql.NullTime{Valid: true, Time: publishedDateTime}, story.Title, content, "", "", time.Now(), time.Now()}

					err = article.createArticle(deps)
					if err != nil {
						sublog.Warn().Err(err).Str("symbol", ticker.TickerSymbol).Msg("failed to write new news article")
					}

					articleTicker := ArticleTicker{0, article.ArticleId, ticker.TickerSymbol, ticker.TickerId, time.Now(), time.Now()}
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

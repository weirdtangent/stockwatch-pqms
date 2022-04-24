package main

import (
	//"flag"
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/weirdtangent/myaws"
)

const (
	startSleepTime = 5.0
	maxSleepTime   = 600.0

	sqlDateTime = "2006-01-02 15:04:05"

	debugging = true
)

type ContextKey string

func main() {
	ctx := context.Background()

	// setup logging -------------------------------------------------------------
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	// alter the caller() return to only include the last directory
	zerolog.CallerMarshalFunc = func(file string, line int) string {
		parts := strings.Split(file, "/")
		if len(parts) > 1 {
			return strings.Join(parts[len(parts)-2:], "/") + ":" + strconv.Itoa(line)
		}
		return file + ":" + strconv.Itoa(line)
	}
	pgmPath := strings.Split(os.Args[0], `/`)
	logTag := "stockwatch-pqms"
	if len(pgmPath) > 1 {
		logTag = pgmPath[len(pgmPath)-1]
	}
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	if debugging {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)

	}
	log.Logger = log.With().Str("@tag", logTag).Caller().Logger()

	// connect to AWS
	awssess, err := myaws.AWSConnect("us-east-1", "stockwatch")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to AWS")
	}
	ctx = context.WithValue(ctx, ContextKey("awssess"), awssess)

	// connect to DB
	db := myaws.DBMustConnect(awssess, "stockwatch")
	ctx = context.WithValue(ctx, ContextKey("db"), db)

	// get msfinance api access key and host
	ms_api_access_key, err := myaws.AWSGetSecretKV(awssess, "stockwatch", "msfinance_rapidapi_key")
	if err != nil {
		log.Fatal().Err(err).
			Msg("failed to get msfinance API key")
	}
	ctx = context.WithValue(ctx, ContextKey("msfinance_apikey"), *ms_api_access_key)

	ms_api_access_host, err := myaws.AWSGetSecretKV(awssess, "stockwatch", "msfinance_rapidapi_host")
	if err != nil {
		log.Fatal().Err(err).
			Msg("failed to get msfinance API key")
	}
	ctx = context.WithValue(ctx, ContextKey("msfinance_apihost"), *ms_api_access_host)

	// get bbfinance api access key and host
	bb_api_access_key, err := myaws.AWSGetSecretKV(awssess, "stockwatch", "bbfinance_rapidapi_key")
	if err != nil || *bb_api_access_key == "" {
		log.Fatal().Err(err).
			Msg("failed to get bbfinance API key")
	}
	ctx = context.WithValue(ctx, ContextKey("bbfinance_apikey"), *bb_api_access_key)

	bb_api_access_host, err := myaws.AWSGetSecretKV(awssess, "stockwatch", "bbfinance_rapidapi_host")
	if err != nil || *bb_api_access_host == "" {
		log.Fatal().Err(err).
			Msg("failed to get bbfinance API key")
	}
	ctx = context.WithValue(ctx, ContextKey("bbfinance_apihost"), *bb_api_access_host)

	// main loop --------------------------------------------------------------
	mainLoop(ctx)
}

func mainLoop(ctx context.Context) {
	var sleepTime float64 = startSleepTime
	var wasProcessed, anyProcessed bool
	var err error

	log.Info().Msg("starting up pqms loop")
	for {
		// wasProcessed, err = getTask(ctx, "stockwatch-tickers-eod")
		// if err != nil {
		// 	log.Fatal().Err(err).Msg("Fatal error, aborting loop")
		// }
		// anyProcessed = anyProcessed || wasProcessed

		wasProcessed, err = getTask(ctx, "stockwatch-tickers-news")
		if err != nil {
			log.Fatal().Err(err).Msg("Fatal error, aborting loop")
		}
		anyProcessed = anyProcessed || wasProcessed

		wasProcessed, err = getTask(ctx, "stockwatch-tickers-financials")
		if err != nil {
			log.Fatal().Err(err).Msg("Fatal error, aborting loop")
		}
		anyProcessed = anyProcessed || wasProcessed

		// if we processed something, restart sleep to 5 sec, but don't
		// even sleep, just go check for another task right away
		if anyProcessed {
			sleepTime = startSleepTime
		} else {
			lastSleepTime := sleepTime
			sleepTime = math.Round(math.Min(sleepTime*1.20, maxSleepTime)*100) / 100
			if lastSleepTime != sleepTime {
				log.Info().Float64("sleep_time", sleepTime).Msg("sleep timer extended to {sleepTime} seconds")
			}
			s, _ := time.ParseDuration(fmt.Sprintf("%.0fs", sleepTime))
			time.Sleep(s)
		}
	}
}

func getTask(ctx context.Context, queueName string) (bool, error) {
	awssess := ctx.Value(ContextKey("awssess")).(*session.Session)

	awssvc := sqs.New(awssess)
	log.Logger = log.With().Str("queue", queueName).Logger()

	taskError := ""

	urlResult, err := awssvc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to get URL for queue")
		return false, err
	}

	// get next message from queue, if any
	queueURL := urlResult.QueueUrl

	msgResult, err := awssvc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            queueURL,
		MaxNumberOfMessages: aws.Int64(1),
		VisibilityTimeout:   aws.Int64(60),
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to get next message in queue")
		return false, err
	}
	if len(msgResult.Messages) == 0 {
		return false, nil
	}

	message := msgResult.Messages[0]
	messageHandle := message.ReceiptHandle
	messageAttributes := message.MessageAttributes

	actionAttr, ok := messageAttributes["action"]
	if !ok {
		log.Error().Msg("missing attribute 'action'")
		taskError = "missing attribute 'action'"
		return deleteTask(ctx, messageHandle, queueURL, taskError)
	}
	action := *(actionAttr.StringValue)
	body := msgResult.Messages[0].Body

	log.Logger = log.With().Str("action", action).Logger()
	log.Info().Msg("received {action} message from queue")
	taskStart := time.Now()

	// go handle whatever type of queued task this is
	var success bool

	// task processor should return:
	// false, err means fatal error, no need to keep retrying
	// false, nil means couldn't process now, but can try again
	//  true, nil means processed
	switch action {
	case "eod":
		success, err = perform_tickers_eod(ctx, body)
	case "intraday":
		success, err = perform_tickers_intraday(ctx, body)
	case "news":
		success, err = perform_tickers_news(ctx, body)
	case "financials":
		success, err = perform_tickers_financials(ctx, body)
	default:
		success = false
		taskError = fmt.Sprintf("unknown action string (%s) in queued task", action)
		return deleteTask(ctx, messageHandle, queueURL, taskError)
	}
	log.Info().Int64("task_time_ns", time.Since(taskStart).Nanoseconds()).Msg("{action} message handled in {task_time_ns} ns")

	if err != nil {
		taskError = "Failed to process queued task, retrying won't help, deleting unprocessable task"
		return deleteTask(ctx, messageHandle, queueURL, taskError)
	} else if !success {
		log.Error().Err(err).Msg("Failed to process queued task successfully, but retryable, leaving for another attempt")
		// returning here without deleting from the queue
		return true, nil
	}

	// task handled, delete message from queue
	return deleteTask(ctx, messageHandle, queueURL, taskError)
}

func deleteTask(ctx context.Context, messageHandle, queueURL *string, taskError string) (bool, error) {
	awssess := ctx.Value(ContextKey("awssess")).(*session.Session)
	awssvc := sqs.New(awssess)

	_, err := awssvc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      queueURL,
		ReceiptHandle: messageHandle,
	})

	// if task had an error message already AND we got another trying to delete, merge into a single error
	if err != nil && taskError != "" {
		err = fmt.Errorf("%s, plus another error when trying to delete the task: %s", taskError, err)
	}

	return (taskError == ""), err
}

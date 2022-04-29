package main

import (
	//"flag"
	"context"
	"fmt"
	"math"
	"os"
	"regexp"
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

	awsRegion            = "us-east-1"
	awsPrivateBucketName = "stockwatch-private"

	sqlDateTime = "2006-01-02 15:04:05"

	debugging = true
)

var (
	// regexs
	absoluteUrl         = regexp.MustCompile(`^https?\://\S+`)
	relativeProtocolUrl = regexp.MustCompile(`^//\S+`)
	getProtocolUrl      = regexp.MustCompile(`^https?\:`)
	relativePathUrl     = regexp.MustCompile(`^/[^/]\S+`)
)

type ContextKey string

func main() {
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
	log := log.Logger.With().Str("@tag", logTag).Caller().Logger()
	ctx := log.WithContext(context.Background())

	// connect to AWS
	awssess, err := myaws.AWSConnect("us-east-1", "stockwatch")
	if err != nil {
		zerolog.Ctx(ctx).Fatal().Err(err).Msg("Failed to connect to AWS")
	}
	ctx = context.WithValue(ctx, ContextKey("awssess"), awssess)

	// connect to DB
	db := myaws.DBMustConnect(awssess, "stockwatch")
	ctx = context.WithValue(ctx, ContextKey("db"), db)

	// get msfinance api access key and host
	ms_api_access_key, err := myaws.AWSGetSecretKV(awssess, "stockwatch", "msfinance_rapidapi_key")
	if err != nil {
		zerolog.Ctx(ctx).Fatal().Err(err).
			Msg("failed to get msfinance API key")
	}
	ctx = context.WithValue(ctx, ContextKey("msfinance_apikey"), *ms_api_access_key)

	ms_api_access_host, err := myaws.AWSGetSecretKV(awssess, "stockwatch", "msfinance_rapidapi_host")
	if err != nil {
		zerolog.Ctx(ctx).Fatal().Err(err).
			Msg("failed to get msfinance API key")
	}
	ctx = context.WithValue(ctx, ContextKey("msfinance_apihost"), *ms_api_access_host)

	// get bbfinance api access key and host
	bb_api_access_key, err := myaws.AWSGetSecretKV(awssess, "stockwatch", "bbfinance_rapidapi_key")
	if err != nil || *bb_api_access_key == "" {
		zerolog.Ctx(ctx).Fatal().Err(err).
			Msg("failed to get bbfinance API key")
	}
	ctx = context.WithValue(ctx, ContextKey("bbfinance_apikey"), *bb_api_access_key)

	bb_api_access_host, err := myaws.AWSGetSecretKV(awssess, "stockwatch", "bbfinance_rapidapi_host")
	if err != nil || *bb_api_access_host == "" {
		zerolog.Ctx(ctx).Fatal().Err(err).
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

	zerolog.Ctx(ctx).Info().Msg("starting up pqms loop")
	for {
		wasProcessed, err = getTask(ctx, "stockwatch-tickers")
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("task failed: {error}")
		}
		anyProcessed = anyProcessed || wasProcessed

		// if we processed something, restart sleep to 5 sec, but don't
		// even sleep, just go check for another task right away
		if anyProcessed {
			sleepTime = startSleepTime
		} else {
			lastSleepTime := sleepTime
			sleepTime = math.Round(math.Min(sleepTime*2, maxSleepTime)*100) / 100
			if lastSleepTime != sleepTime {
				zerolog.Ctx(ctx).Info().Float64("sleep_time", sleepTime).Msg("sleep timer extended to {sleepTime} seconds")
			}
			s, _ := time.ParseDuration(fmt.Sprintf("%.0fs", sleepTime))
			time.Sleep(s)
		}
	}
}

func getTask(ctx context.Context, queueName string) (bool, error) {
	awssess := ctx.Value(ContextKey("awssess")).(*session.Session)

	awssvc := sqs.New(awssess)
	log := zerolog.Ctx(ctx).With().Str("queue", queueName).Logger()
	ctx = log.WithContext(ctx)

	taskError := ""

	urlResult, err := awssvc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to get URL for queue")
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
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to get next message in queue")
		return false, err
	}

	// no messages to handle
	if len(msgResult.Messages) == 0 {
		return false, nil
	}

	message := msgResult.Messages[0]
	messageHandle := message.ReceiptHandle
	messageAttributes := message.MessageAttributes

	actionAttr, ok := messageAttributes["action"]
	if !ok {
		zerolog.Ctx(ctx).Error().Msg("missing attribute 'action'")
		taskError = "missing attribute 'action'"
		deleteTask(ctx, messageHandle, queueURL, taskError)
		return true, nil
	}
	action := *(actionAttr.StringValue)
	body := msgResult.Messages[0].Body

	log = zerolog.Ctx(ctx).With().Str("action", action).Logger()
	ctx = log.WithContext(ctx)
	zerolog.Ctx(ctx).Info().Msg("received {action} message from queue")
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
	case "favicon":
		success, err = perform_tickers_favicon(ctx, body)
	default:
		success = false
		taskError = fmt.Sprintf("unknown action string (%s) in queued task", action)
		deleteTask(ctx, messageHandle, queueURL, taskError)
		return true, nil
	}

	if success {
		// task handled, delete message from queue
		log.Info().Int64("response_time", time.Since(taskStart).Nanoseconds()).Msg("another '{action}' message handled successfully, took {response_time} ns")
		deleteTask(ctx, messageHandle, queueURL, taskError)
		return true, nil
	}
	if err != nil {
		taskError = "failed to process message, retrying won't help, deleting unprocessable task"
		log.Info().Int64("response_time", time.Since(taskStart).Nanoseconds()).Msg("failed to process '{action}' message successfully ({error}), but retryable so leaving for another attempt")
		deleteTask(ctx, messageHandle, queueURL, taskError)
		return true, nil
	}

	zerolog.Ctx(ctx).Info().Msg("failed to process '{action}' message successfully, but retryable so leaving for another attempt")
	return false, nil
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

package main

import (
	"fmt"
	"math"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
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
	deps := &Dependencies{}

	setupLogging(deps)
	setupAWS(deps)
	setupSecrets(deps)

	mainLoop(deps)
}

func mainLoop(deps *Dependencies) {
	sublog := deps.logger

	var sleepTime float64 = startSleepTime
	var wasProcessed, anyProcessed bool
	var err error

	sublog.Info().Msg("starting up pqms loop")
	timer := time.Now()
	count := 0
	for {
		// if we checked more than 1/sec over the last minute
		// force a 5 minute pause and set sleep to max!
		if time.Since(timer).Minutes() > 1.0 {
			if count > 60 {
				sublog.Warn().Int("count", count).Float64("min", time.Since(timer).Minutes()).Msg("exceeded max check, {count} over last {min} mins, pausing!")
				time.Sleep(5 * time.Minute)
				sleepTime = maxSleepTime
			}
			count = 0
			timer = time.Now()
		}

		wasProcessed, err = getTask(deps, "stockwatch-tickers")
		if err != nil {
			sublog.Error().Err(err).Msg("task failed: {error}")
		}
		anyProcessed = anyProcessed || wasProcessed
		count++

		// if we processed something, restart sleep to 5 sec, but don't
		// even sleep, just go check for another task right away
		if anyProcessed {
			sleepTime = startSleepTime
		} else {
			lastSleepTime := sleepTime
			sleepTime = math.Round(math.Min(sleepTime*2, maxSleepTime)*100) / 100
			if lastSleepTime != sleepTime {
				sublog.Info().Float64("sleep_time", sleepTime).Msg("sleep timer extended to {sleepTime} seconds")
			}
			s, _ := time.ParseDuration(fmt.Sprintf("%.0fs", sleepTime))
			time.Sleep(s)
		}
	}
}

func getTask(deps *Dependencies, queueName string) (bool, error) {
	awssess := deps.awssess
	sublog := deps.logger

	awssvc := sqs.New(awssess)

	taskError := ""

	urlResult, err := awssvc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})
	if err != nil {
		sublog.Error().Err(err).Msg("failed to get URL for queue")
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
		sublog.Error().Err(err).Msg("failed to get next message in queue")
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
		sublog.Error().Msg("missing attribute 'action'")
		taskError = "missing attribute 'action'"
		deleteTask(deps, messageHandle, queueURL, taskError)
		return true, nil
	}
	action := *(actionAttr.StringValue)
	body := msgResult.Messages[0].Body

	newdeps := deps
	newlog := deps.logger.With().Str("action", action).Logger()
	newdeps.logger = &newlog
	sublog = &newlog

	sublog.Info().Msg("received {action} message from queue")

	taskStart := time.Now()

	// go handle whatever type of queued task this is
	var success bool

	// task processor should return:
	// false, err means fatal error, no need to keep retrying
	// false, nil means couldn't process now, but can try again
	//  true, nil means processed
	switch action {
	case "eod":
		success, err = perform_tickers_eod(newdeps, body)
	case "intraday":
		success, err = perform_tickers_intraday(newdeps, body)
	case "news":
		success, err = perform_tickers_news(newdeps, body)
	case "financials":
		success, err = perform_tickers_financials(newdeps, body)
	case "favicon":
		success, err = perform_tickers_favicon(newdeps, body)
	default:
		success = false
		taskError = fmt.Sprintf("unknown action string (%s) in queued task", action)
		deleteTask(newdeps, messageHandle, queueURL, taskError)
		return true, nil
	}

	if success {
		// task handled, delete message from queue
		sublog.Info().Int64("response_time", time.Since(taskStart).Nanoseconds()).Msg("another '{action}' message handled successfully, took {response_time} ns")
		deleteTask(newdeps, messageHandle, queueURL, taskError)
		return true, nil
	}
	if err != nil {
		taskError = "failed to process message, retrying won't help, deleting unprocessable task"
		sublog.Info().Int64("response_time", time.Since(taskStart).Nanoseconds()).Msg("failed to process '{action}' message successfully ({error}), but retryable so leaving for another attempt")
		deleteTask(newdeps, messageHandle, queueURL, taskError)
		return true, nil
	}

	sublog.Info().Msg("failed to process '{action}' message successfully, but retryable so leaving for another attempt")
	return false, nil
}

func deleteTask(deps *Dependencies, messageHandle, queueURL *string, taskError string) (bool, error) {
	awssess := deps.awssess

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

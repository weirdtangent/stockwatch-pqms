package main

import (
	//"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/weirdtangent/myaws"
)

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
	log.Logger = log.With().Caller().Logger()
	log.Info().Msg("logging setup complete")

	// connect to AWS
	awssess, err := myaws.AWSConnect("us-east-1", "stockwatch")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to AWS")
	}
	log.Info().Msg("AWS session setup complete")

	// connect to Aurora
	db, err := myaws.DBConnect(awssess, "stockwatch_rds", "stockwatch")
	if err != nil || db == nil {
		log.Fatal().Err(err).Msg("Failed to connect to RDS")
	}
	log.Info().Msg("MySQL connection complete")

	mainloop(awssess, db)
}

func mainloop(awssess *session.Session, db *sqlx.DB) {
	var sleepTime float64 = 5
	for {
		processed, err := gettask(awssess, db, "stockwatch-tickers-eod")
		if err != nil {
			log.Fatal().Err(err).Msg("Fatal error, aborting loop")
		}
		// if we processed something, restart sleep to 5 sec, but don't
		// even sleep, just go check for another task right away
		if processed == true {
			sleepTime = 5
		} else {
			if sleepTime < 60 {
				sleepTime *= 1.20
			}
			s, _ := time.ParseDuration(fmt.Sprintf("%.0fs", sleepTime))
			time.Sleep(s)
		}
	}
}

func gettask(awssess *session.Session, db *sqlx.DB, queueName string) (bool, error) {
	awssvc := sqs.New(awssess)
	tasklog := log.With().Str("queue", queueName).Logger()

	urlResult, err := awssvc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})
	if err != nil {
		tasklog.Error().Err(err).Msg("Failed to get URL for queue")
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
		tasklog.Error().Err(err).Msg("Failed to get next message in queue")
		return false, err
	}
	if len(msgResult.Messages) == 0 {
		return false, nil
	}

	message := msgResult.Messages[0]
	messageHandle := message.ReceiptHandle

	action := *message.MessageAttributes["action"].StringValue
	body := msgResult.Messages[0].Body
	tasklog = tasklog.With().Str("action", action).Logger()
	tasklog.Info().Msg("Handling message in queue")

	// go handle whatever type of queued task this is
	var success bool

	// task processor should return:
	// false, err means fatal error, no need to keep retrying
	// false, nil means couldn't process now, but can try again
	//  true, nil means processed
	switch action {
	case "eod":
		success, err = perform_tickers_eod(awssess, db, body)
	case "intraday":
		success, err = perform_tickers_intraday(awssess, db, body)
	default:
		success = false
		err = fmt.Errorf("unknown action: %s", action)
		tasklog.Error().Msg("Failed to understand action for this task type")
	}

	if err != nil {
		tasklog.Error().Err(err).Msg("Failed to process queued task, retrying won't help, deleting unprocessable task")
	} else if success == false {
		tasklog.Error().Err(err).Msg("Failed to process queued task successfully, but retryable")
		return true, nil
	}

	// if handled successfully, or not but retrying can't possibly help, delete message from queue
	_, err = awssvc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      queueURL,
		ReceiptHandle: messageHandle,
	})
	if err != nil {
		tasklog.Error().Err(err).Msg("Failed to delete message after processing")
		return true, nil
	}

	return true, nil
}

func perform_tickers_intraday(awssess *session.Session, db *sqlx.DB, body *string) (bool, error) {
	if body != nil {
		return false, fmt.Errorf("Just testing")
	}
	return false, fmt.Errorf("Just testing")
}

func perform_tickers_deadletter(awssess *session.Session, db *sqlx.DB, body *string) (bool, error) {
	if body != nil {
		return false, fmt.Errorf("Just testing")
	}
	return false, fmt.Errorf("Just testing")
}

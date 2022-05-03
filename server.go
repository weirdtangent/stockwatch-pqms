package main

import (
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/weirdtangent/myaws"
)

type Dependencies struct {
	awssess *session.Session
	db      *sqlx.DB
	logger  *zerolog.Logger
	secrets map[string]string
}

func setupLogging(deps *Dependencies) {
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
	if debugging {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)

	}
	newlog := log.With().Str("@tag", logTag).Caller().Logger()

	deps.logger = &newlog
}

func setupAWS(deps *Dependencies) {
	deps.awssess = myaws.AWSMustConnect("us-east-1", "stockwatch")
	deps.db = myaws.DBMustConnect(deps.awssess, "stockwatch")
}

func setupSecrets(deps *Dependencies) {
	sublog := deps.logger
	awssess := deps.awssess

	secretValues, err := myaws.AWSGetSecret(awssess, "stockwatch")
	if err != nil {
		sublog.Fatal().Err(err)
	}

	secrets := make(map[string]string)
	for key := range secretValues {
		value := secretValues[key]
		secrets[key] = value
	}

	deps.secrets = secrets
}

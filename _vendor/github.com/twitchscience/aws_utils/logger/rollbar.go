package logger

import (
	"errors"

	"github.com/sirupsen/logrus"
	"github.com/stvp/rollbar"
)

// NewRollbarHook creates a Rollbar hook for logrus.
func NewRollbarHook(token string, env string, levels []logrus.Level) logrus.Hook {
	if token == "" || env == "" {
		return nil
	}
	rollbar.Token = token
	rollbar.Environment = env
	rollbar.ErrorWriter = nil // default is stderr with fmt.Printf
	return &rollbarHook{levels: levels}
}

type rollbarHook struct {
	levels []logrus.Level
}

func (rh *rollbarHook) Fire(e *logrus.Entry) error {
	level := getRollbarLevel(e.Level)
	err := errors.New(e.Message)

	var fields []*rollbar.Field
	for k, v := range e.Data {
		switch val := v.(type) {
		case error:
			// rollbar uses JSON.Marshal on the error, which renders to "{}", so
			// turn it to a string here.
			v = val.Error()
		}
		fields = append(fields, &rollbar.Field{
			Name: k,
			Data: v,
		})
	}

	rollbar.ErrorWithStackSkip(level, err, 5, fields...)
	return nil
}

func (rh *rollbarHook) Levels() []logrus.Level {
	return rh.levels
}

func getRollbarLevel(level logrus.Level) string {
	switch level {
	case logrus.FatalLevel,
		logrus.PanicLevel:
		return rollbar.CRIT
	case logrus.ErrorLevel:
		return rollbar.ERR
	case logrus.WarnLevel:
		return rollbar.WARN
	case logrus.InfoLevel:
		return rollbar.INFO
	case logrus.DebugLevel:
		return rollbar.DEBUG
	default:
		return rollbar.INFO
	}
}

// Package logger is a wrapper around logrus that logs in a
// structured JSON format and provides additional context keys.
// To use this library, first call `logger.Init("info")`, then
// call logging functions e.g. `logger.Infoln("message")`.
// You can also use CaptureDefault() to capture output
// that would go to the default logger (e.g. from dependencies).
package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/sirupsen/logrus"
)

const (
	timestampFormat = "2006-01-02T15:04:05.000000Z-0700"
	defaultDepth    = 4
	withDepth       = 3
	callerKey       = "caller"
)

var logger = logrus.New()
var context = make(map[string]string)

// Init sets up logging at the given level.
func Init(level string) {
	logger.Formatter = &logrus.JSONFormatter{
		TimestampFormat: timestampFormat,
	}
	l, err := logrus.ParseLevel(level)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to setup logging: %v\n", err)
		os.Exit(1)
	}
	logger.Level = l
	context["env"] = getEnv("CLOUD_ENVIRONMENT", "local")
	context["pid"] = strconv.Itoa(os.Getpid())
	context["host"], err = os.Hostname()
	if err != nil {
		context["host"] = getEnv("HOST", "localhost")
	}
}

// getEnv returns the given environment variable or the default if it is undefined.
func getEnv(name, deflt string) string {
	val, ok := os.LookupEnv(name)
	if !ok {
		return deflt
	}
	return val
}

// addContext adds the caller and context map to the log data map.
func addContext(data map[string]interface{}, depth int) {
	// TODO(dwe): Use logrus to handle this? https://github.com/Sirupsen/logrus/issues/63
	data[callerKey] = caller(depth)

	for k, v := range context {
		data[k] = v
	}
}

// caller gives the filename/line at the given depth.
func caller(depth int) (str string) {
	_, file, line, ok := runtime.Caller(depth)
	if !ok {
		str = "???:0"
	} else {
		str = fmt.Sprint(filepath.Base(file), ":", line)
	}
	return
}

// newEntry creates a new logrus.Entry with context added to it.
func newEntry() *logrus.Entry {
	entry := logrus.NewEntry(logger)
	addContext(entry.Data, defaultDepth)
	return entry
}

// WithField creates an Entry with the given key/value added.
func WithField(key string, value interface{}) *logrus.Entry {
	e := logger.WithField(key, value)
	addContext(e.Data, withDepth)
	return e
}

// WithFields creates an Entry with the given fields added.
func WithFields(fields map[string]interface{}) *logrus.Entry {
	e := logger.WithFields(fields)
	addContext(e.Data, withDepth)
	return e
}

// WithError adds the error to the `error` key.
func WithError(err error) *logrus.Entry {
	e := logger.WithError(err)
	addContext(e.Data, withDepth)
	return e
}

// Debugf logs the formatted string at Debug level.
func Debugf(format string, args ...interface{}) {
	if logger.Level >= logrus.DebugLevel {
		newEntry().Debugf(format, args...)
	}
}

// Infof logs the formatted string at Info level.
func Infof(format string, args ...interface{}) {
	if logger.Level >= logrus.InfoLevel {
		newEntry().Infof(format, args...)
	}
}

// Printf logs the formatted string at Info level.
func Printf(format string, args ...interface{}) {
	if logger.Level >= logrus.InfoLevel {
		newEntry().Printf(format, args...)
	}
}

// Warnf logs the formatted string at Warn level.
func Warnf(format string, args ...interface{}) {
	if logger.Level >= logrus.WarnLevel {
		newEntry().Warnf(format, args...)
	}
}

// Warningf logs the formatted string at Warn level.
func Warningf(format string, args ...interface{}) {
	if logger.Level >= logrus.WarnLevel {
		newEntry().Warnf(format, args...)
	}
}

// Errorf logs the formatted string at Error level.
func Errorf(format string, args ...interface{}) {
	if logger.Level >= logrus.ErrorLevel {
		newEntry().Errorf(format, args...)
	}
}

// Fatalf logs the formatted string at Fatal level and exits.
func Fatalf(format string, args ...interface{}) {
	newEntry().Fatalf(format, args...)
}

// Panicf logs the formatted string at Panic level and panics.
func Panicf(format string, args ...interface{}) {
	newEntry().Panicf(format, args...)
}

// Debug logs the args at the Debug level.
func Debug(args ...interface{}) {
	if logger.Level >= logrus.DebugLevel {
		newEntry().Debug(args...)
	}
}

// Info logs the args at the Info level.
func Info(args ...interface{}) {
	if logger.Level >= logrus.InfoLevel {
		newEntry().Info(args...)
	}
}

// Print logs the args at the Info level.
func Print(args ...interface{}) {
	if logger.Level >= logrus.InfoLevel {
		newEntry().Info(args...)
	}
}

// Warn logs the args at the Warn level.
func Warn(args ...interface{}) {
	if logger.Level >= logrus.WarnLevel {
		newEntry().Warn(args...)
	}
}

// Warning logs the args at the Warn level.
func Warning(args ...interface{}) {
	if logger.Level >= logrus.WarnLevel {
		newEntry().Warn(args...)
	}
}

// Error logs the args at the Error level.
func Error(args ...interface{}) {
	if logger.Level >= logrus.ErrorLevel {
		newEntry().Error(args...)
	}
}

// Fatal logs the args at the Fatal level and exits.
func Fatal(args ...interface{}) {
	newEntry().Fatal(args...)
}

// Panic logs the args at the Fatal level and panics.
func Panic(args ...interface{}) {
	newEntry().Panic(args...)
}

// Debugln logs the args at the Debug level.
func Debugln(args ...interface{}) {
	if logger.Level >= logrus.DebugLevel {
		newEntry().Debugln(args...)
	}
}

// Infoln logs the args at the Info level.
func Infoln(args ...interface{}) {
	if logger.Level >= logrus.InfoLevel {
		newEntry().Infoln(args...)
	}
}

// Println logs the args at the Info level.
func Println(args ...interface{}) {
	if logger.Level >= logrus.InfoLevel {
		newEntry().Println(args...)
	}
}

// Warnln logs the args at the Warn level.
func Warnln(args ...interface{}) {
	if logger.Level >= logrus.WarnLevel {
		newEntry().Warnln(args...)
	}
}

// Warningln logs the args at the Warn level.
func Warningln(args ...interface{}) {
	if logger.Level >= logrus.WarnLevel {
		newEntry().Warnln(args...)
	}
}

// Errorln logs the args at the Error level.
func Errorln(args ...interface{}) {
	if logger.Level >= logrus.ErrorLevel {
		newEntry().Errorln(args...)
	}
}

// Fatalln logs the args at the Fatal level and exits.
func Fatalln(args ...interface{}) {
	newEntry().Fatalln(args...)
}

// Panicln logs the args at the Panic level and exits.
func Panicln(args ...interface{}) {
	newEntry().Panicln(args...)
}

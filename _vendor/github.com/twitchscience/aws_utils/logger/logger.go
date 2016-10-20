// Package logger is a wrapper around logrus that logs in a
// structured JSON format and provides additional context keys.
// To use this library, first call `logger.Init("info")` or
// logger.InitWithRollbar(...), then call logging functions
// e.g. `logger.Infoln("message")`.  You can also use
// CaptureDefault() to capture output that would go to the
// default logger (e.g. from dependencies).
package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stvp/rollbar"
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

// InitWithRollbar sets up logging at the given level, and sends log message of
// level panic, fatal, and error to rollbar. If rollbarToken isn't properly set,
// it will initialize the logrus logger without rollbar and move on, logging
// a message once the error response comes back from rollbar.
// In your main shutdown code, be sure to call rollbar.Wait() to guarantee late
// errors get sent to rollbar.
func InitWithRollbar(level, rollbarToken, rollbarEnv string) {
	Init(level)
	hook := NewRollbarHook(rollbarToken, rollbarEnv,
		[]logrus.Level{
			logrus.PanicLevel,
			logrus.FatalLevel,
			logrus.ErrorLevel,
		})
	if hook != nil {
		logger.Hooks.Add(hook)

		rollbar.Message("info", "Initializing!")

		go func() {
			timeout := make(chan bool, 1)
			go func() {
				time.Sleep(5 * time.Second)
				timeout <- true
			}()
			select {
			case err := <-rollbar.PostErrors():
				err = err
				// WithError(err).Error("Rollbar message failed to send")
			case <-timeout:
			}
		}()
	}
}

// getEnv returns the given environment variable or the default if it is undefined.
func getEnv(name, deflt string) string {
	if val, ok := os.LookupEnv(name); ok {
		return val
	}
	return deflt
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
func caller(depth int) string {
	if _, file, line, ok := runtime.Caller(depth); ok {
		return fmt.Sprint(filepath.Base(file), ":", line)
	}
	return "???:0"
}

// newEntry creates a new Entry with context added to it.
func newEntry() *Entry {
	e := logrus.NewEntry(logger)
	addContext(e.Data, defaultDepth)
	return &Entry{*e}
}

// WithField creates an Entry with the given key/value added.
func WithField(key string, value interface{}) *Entry {
	return newEntry().WithField(key, value)
}

// WithFields creates an Entry with the given fields added.
func WithFields(fields map[string]interface{}) *Entry {
	return newEntry().WithFields(fields)
}

// WithError adds the error to the `error` key.
func WithError(err error) *Entry {
	return newEntry().WithError(err)
}

func safeRollbarWait(timeout time.Duration) {
	rollbarC := make(chan struct{})
	go func() {
		rollbar.Wait()
		close(rollbarC)
	}()
	select {
	case <-rollbarC:
	case <-time.After(timeout):
	}
}

// Wait waits to finish sending logs to rollbar
func Wait() {
	safeRollbarWait(time.Second * 5)
}

// Go spawns a goroutine like `go`, but captures and logs any panics
// that happen in them. Turn `go g()` into `logger.Go(func(){g()})
func Go(f func()) {
	go func() {
		defer LogPanic()
		f()
	}()
}

// LogPanic catches any raw panics and logs them. Triggers another panic.
func LogPanic() {
	if rec := recover(); rec != nil {
		switch e := rec.(type) {
		case error:
			WithError(e).Panic(e.Error())
		default:
			Panicf("%v", rec)
		}
	}
}

// Print family functions:

// Debug logs the args at the Debug level.
func Debug(args ...interface{}) {
	newEntry().Debug(args...)
}

// Info logs the args at the Info level.
func Info(args ...interface{}) {
	newEntry().Info(args...)
}

// Print logs the args at the Info level.
func Print(args ...interface{}) {
	newEntry().Print(args...)
}

// Warn logs the args at the Warn level.
func Warn(args ...interface{}) {
	newEntry().Warn(args...)
}

// Warning logs the args at the Warn level.
func Warning(args ...interface{}) {
	newEntry().Warn(args...)
}

// Error logs the args at the Error level.
func Error(args ...interface{}) {
	newEntry().Error(args...)
}

// Fatal logs the args at the Error level and exits.
func Fatal(args ...interface{}) {
	newEntry().Fatal(args...)
}

// Panic logs the args at the Error level and panics.
func Panic(args ...interface{}) {
	newEntry().Panic(args...)
}

// Printf family functions:

// Debugf logs the formatted string at Debug level.
func Debugf(format string, args ...interface{}) {
	newEntry().Debugf(format, args...)
}

// Infof logs the formatted string at Info level.
func Infof(format string, args ...interface{}) {
	newEntry().Infof(format, args...)
}

// Printf logs the formatted string at Info level.
func Printf(format string, args ...interface{}) {
	newEntry().Printf(format, args...)
}

// Warnf logs the formatted string at Warn level.
func Warnf(format string, args ...interface{}) {
	newEntry().Warnf(format, args...)
}

// Warningf logs the formatted string at Warn level.
func Warningf(format string, args ...interface{}) {
	newEntry().Warnf(format, args...)
}

// Errorf logs the formatted string at Error level.
func Errorf(format string, args ...interface{}) {
	newEntry().Errorf(format, args...)
}

// Fatalf logs the formatted string at Error level and exits.
func Fatalf(format string, args ...interface{}) {
	newEntry().Fatalf(format, args...)
}

// Panicf logs the formatted string at Error level and panics.
func Panicf(format string, args ...interface{}) {
	newEntry().Panicf(format, args...)
}

// Println family functions:

// Debugln logs the args at the Debug level.
func Debugln(args ...interface{}) {
	newEntry().Debugln(args...)
}

// Infoln logs the args at the Info level.
func Infoln(args ...interface{}) {
	newEntry().Infoln(args...)
}

// Println logs the args at the Info level.
func Println(args ...interface{}) {
	newEntry().Println(args...)
}

// Warnln logs the args at the Warn level.
func Warnln(args ...interface{}) {
	newEntry().Warnln(args...)
}

// Warningln logs the args at the Warn level.
func Warningln(args ...interface{}) {
	newEntry().Warnln(args...)
}

// Errorln logs the args at the Error level.
func Errorln(args ...interface{}) {
	newEntry().Errorln(args...)
}

// Fatalln logs the args at the Error level and exits.
func Fatalln(args ...interface{}) {
	newEntry().Fatalln(args...)
}

// Panicln logs the args at the Error level and panics.
func Panicln(args ...interface{}) {
	newEntry().Panicln(args...)
}

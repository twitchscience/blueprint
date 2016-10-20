package logger

import (
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	errorLevelKey = "errorLevel"
)

type Entry struct {
	logrus.Entry
}

func (entry *Entry) WithError(err error) *Entry {
	return &Entry{*entry.Entry.WithError(err)}
}
func (entry *Entry) WithField(key string, value interface{}) *Entry {
	return &Entry{*entry.Entry.WithField(key, value)}
}
func (entry *Entry) WithFields(fields logrus.Fields) *Entry {
	return &Entry{*entry.Entry.WithFields(fields)}
}

// Entry Print family functions:

// Debug logs the args at the Debug level.
func (entry *Entry) Debug(args ...interface{}) {
	if logger.Level >= logrus.DebugLevel {
		entry.Entry.Debug(args...)
	}
}

// Info logs the args at the Info level.
func (entry *Entry) Info(args ...interface{}) {
	if logger.Level >= logrus.InfoLevel {
		entry.Entry.Info(args...)
	}
}

// Print logs the args at the Info level.
func (entry *Entry) Print(args ...interface{}) {
	entry.Info(args...)
}

// Warn logs the args at the Warn level.
func (entry *Entry) Warn(args ...interface{}) {
	if logger.Level >= logrus.WarnLevel {
		entry.Entry.Warn(args...)
	}
}

// Warning logs the args at the Warn level.
func (entry *Entry) Warning(args ...interface{}) {
	if logger.Level >= logrus.WarnLevel {
		entry.Entry.Warn(args...)
	}
}

// Error logs the args at the Error level.
func (entry *Entry) Error(args ...interface{}) {
	if logger.Level >= logrus.ErrorLevel {
		entry.WithField(errorLevelKey, "error").Entry.Error(args...)
	}
}

// Fatal logs the args at the Error level and exits.
func (entry *Entry) Fatal(args ...interface{}) {
	entry.WithField(errorLevelKey, "fatal").Entry.Error(args...)
	safeRollbarWait(time.Second)
	os.Exit(1)
}

// Panic logs the args at the Error level and panics.
func (entry *Entry) Panic(args ...interface{}) {
	entry.WithField(errorLevelKey, "panic").Entry.Error(args...)
	safeRollbarWait(time.Second)
	panic(fmt.Sprint(args...))
}

// Entry Printf family functions:

// Debugf logs the formatted string at Debug level.
func (entry *Entry) Debugf(format string, args ...interface{}) {
	if logger.Level >= logrus.DebugLevel {
		entry.Entry.Debugf(format, args...)
	}
}

// Infof logs the formatted string at Info level.
func (entry *Entry) Infof(format string, args ...interface{}) {
	if logger.Level >= logrus.InfoLevel {
		entry.Entry.Infof(format, args...)
	}
}

// Printf logs the formatted string at Info level.
func (entry *Entry) Printf(format string, args ...interface{}) {
	entry.Infof(format, args...)
}

// Warnf logs the formatted string at Warn level.
func (entry *Entry) Warnf(format string, args ...interface{}) {
	if logger.Level >= logrus.WarnLevel {
		entry.Entry.Warnf(format, args...)
	}
}

// Warningf logs the formatted string at Warn level.
func (entry *Entry) Warningf(format string, args ...interface{}) {
	if logger.Level >= logrus.WarnLevel {
		entry.Entry.Warnf(format, args...)
	}
}

// Errorf logs the formatted string at Error level.
func (entry *Entry) Errorf(format string, args ...interface{}) {
	if logger.Level >= logrus.ErrorLevel {
		entry.WithField(errorLevelKey, "error").Entry.Errorf(format, args...)
	}
}

// Fatalf logs the formatted string at Error level and exits.
func (entry *Entry) Fatalf(format string, args ...interface{}) {
	entry.WithField(errorLevelKey, "fatal").Entry.Errorf(format, args...)
	safeRollbarWait(time.Second)
	os.Exit(1)
}

// Panicf logs the formatted string at Error level and panics.
func (entry *Entry) Panicf(format string, args ...interface{}) {
	entry.WithField(errorLevelKey, "panic").Entry.Errorf(format, args...)
	safeRollbarWait(time.Second)
	panic(fmt.Sprint(args...))
}

// Entry Println family functions:

// Debugln logs the args at the Debug level.
func (entry *Entry) Debugln(args ...interface{}) {
	if logger.Level >= logrus.DebugLevel {
		entry.Entry.Debugln(args...)
	}
}

// Infoln logs the args at the Info level.
func (entry *Entry) Infoln(args ...interface{}) {
	if logger.Level >= logrus.InfoLevel {
		entry.Entry.Infoln(args...)
	}
}

// Println logs the args at the Info level.
func (entry *Entry) Println(args ...interface{}) {
	entry.Infoln(args...)
}

// Warnln logs the args at the Warn level.
func (entry *Entry) Warnln(args ...interface{}) {
	if logger.Level >= logrus.WarnLevel {
		entry.Entry.Warnln(args...)
	}
}

// Warningln logs the args at the Warn level.
func (entry *Entry) Warningln(args ...interface{}) {
	if logger.Level >= logrus.WarnLevel {
		entry.Entry.Warnln(args...)
	}
}

// Errorln logs the args at the Error level.
func (entry *Entry) Errorln(args ...interface{}) {
	if logger.Level >= logrus.ErrorLevel {
		entry.WithField(errorLevelKey, "error").Entry.Errorln(args...)
	}
}

// Fatalln logs the args at the Error level and exits.
func (entry *Entry) Fatalln(args ...interface{}) {
	entry.WithField(errorLevelKey, "fatal").Entry.Errorln(args...)
	safeRollbarWait(time.Second)
	os.Exit(1)
}

// Panicln logs the args at the Error level and panics.
func (entry *Entry) Panicln(args ...interface{}) {
	entry.WithField(errorLevelKey, "panic").Entry.Errorln(args...)
	safeRollbarWait(time.Second)
	panic(fmt.Sprint(args...))
}

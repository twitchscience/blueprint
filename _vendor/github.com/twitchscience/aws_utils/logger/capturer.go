package logger

import (
	"io"
	"log"
	"strings"
)

const (
	prefix = "_log_delimiter_ "
	flags  = log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile
)

func redirect(r io.Reader) {
	buf := make([]byte, 0, 4096)
	leftover := ""
	for {
		n, err := r.Read(buf[:cap(buf)])
		if err != nil {
			logger.Errorf("error redirecting log output: %v", err)
			break
		}
		s := string(buf[:n])
		messages := strings.Split(s, prefix)
		if leftover != "" {
			messages[0] = leftover + messages[0]
		}
		leftover = ""
		for i, msg := range messages {
			if i == 0 && msg == "" {
				continue
			}
			if msg[len(msg)-1] != '\n' {
				leftover = msg
				break
			}
			pieces := strings.SplitN(msg, " ", 3)
			if len(pieces) != 3 {
				redirectedWarn("unknown", "unknown", msg)
				continue
			}
			ts := pieces[0] + " " + pieces[1]
			// Assume filenames don't have ":" in them...
			pieces = strings.SplitN(pieces[2], ":", 3)
			if len(pieces) != 3 {
				redirectedWarn(ts, "unknown", msg)
				continue
			}
			redirectedWarn(ts, pieces[0]+":"+pieces[1], pieces[2][1:len(pieces[2])-1])
		}
	}
}

// redirectedWarn writes a message redirected from a log.Logger at Warn level.
func redirectedWarn(time string, caller string, message string) {
	logger.WithFields(map[string]interface{}{callerKey: caller, "time": time}).Warn(message)
}

// CaptureDefault causes all outupt to the default logger to be redirected
// to the logrus logger at the Warn level.
func CaptureDefault() {
	log.SetFlags(flags)
	log.SetPrefix(prefix)
	r, w := io.Pipe()
	log.SetOutput(w)
	go redirect(r)
}

// GetCapturedLogger returns a *log.Logger with its output redirected
// to the logrus logger at the Warn level.
func GetCapturedLogger() *log.Logger {
	r, w := io.Pipe()
	l := log.New(w, prefix, flags)
	go redirect(r)
	return l
}

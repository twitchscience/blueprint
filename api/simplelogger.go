package api

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"
	"github.com/zenazn/goji/web/mutil"
)

// SimpleLogger is a custom middleware logger that doesn't add colour
func SimpleLogger(c *web.C, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := middleware.GetReqID(*c)

		printStart(reqID, r)

		lw := mutil.WrapWriter(w)

		t1 := time.Now()
		h.ServeHTTP(lw, r)

		if lw.Status() == 0 {
			lw.WriteHeader(http.StatusOK)
		}
		t2 := time.Now()

		printEnd(reqID, lw, t2.Sub(t1))
	})
}

func printStart(reqID string, r *http.Request) {
	header := r.Header.Get("X-Forwarded-For")
	var clientIP string
	comma := strings.LastIndex(header, ",")
	if comma > -1 {
		clientIP = header[comma+1:]
	} else {
		clientIP = header
	}

	fields := map[string]interface{}{
		"request_method": r.Method,
		"url":            r.URL.String(),
		"remote_address": strings.TrimSpace(clientIP),
	}
	if reqID != "" {
		fields["request_id"] = reqID
	}
	logger.WithFields(fields).Info("Started request")
}

func printEnd(reqID string, w mutil.WriterProxy, dt fmt.Stringer) {
	fields := map[string]interface{}{
		"status":   w.Status(),
		"duration": dt.String(),
	}
	if reqID != "" {
		fields["request_id"] = reqID
	}
	logger.WithFields(fields).Info("Completed request")
}

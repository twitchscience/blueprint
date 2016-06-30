package api

import (
	"net/http"
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
	fields := map[string]interface{} {
		"request_method":	r.Method,
		"url":			r.URL.String(),
		"remote_address":	r.RemoteAddr,
	}
	if reqID != "" {
		fields["request_id"] = reqID
	}
	logger.WithFields(fields).Info("Started request")
}

func printEnd(reqID string, w mutil.WriterProxy, dt time.Duration) {
	fields := map[string]interface{} {
		"status":	w.Status(),
		"duration":	dt,
	}
	if reqID != "" {
		fields["request_id"] = reqID
	}
	logger.WithFields(fields).Info("Completed request")
}

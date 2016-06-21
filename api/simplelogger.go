package api

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"
	"github.com/zenazn/goji/web/mutil"
)

// SimpleLogger is a custom middleware logger that doesn't add colour
func SimpleLogger(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
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
	}

	return http.HandlerFunc(fn)
}

func printStart(reqID string, r *http.Request) {
	var buf bytes.Buffer

	if reqID != "" {
		fmt.Fprintf(&buf, "[%s] ", reqID)
	}
	fmt.Fprintf(&buf, "Started %s %q from %s", r.Method, r.URL.String(), r.RemoteAddr)

	log.Print(buf.String())
}

func printEnd(reqID string, w mutil.WriterProxy, dt time.Duration) {
	var buf bytes.Buffer

	if reqID != "" {
		fmt.Fprintf(&buf, "[%s] ", reqID)
	}
	status := w.Status()
	fmt.Fprintf(&buf, "Returning %03d in %s", status, dt)

	log.Print(buf.String())
}

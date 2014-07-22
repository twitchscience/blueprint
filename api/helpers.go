package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

func path(root, file string) string {
	if file == "/" {
		file = "/index.html"
	}
	return fmt.Sprintf("%s/%s", root, file)
}

func fourOhFour(w http.ResponseWriter, r *http.Request) {
	http.Error(w, http.StatusText(404), 404)
}

func writeEvent(w http.ResponseWriter, events []scoop_protocol.Config) {
	b, err := json.Marshal(events)
	if err != nil {
		log.Println("Error serializing data")
		http.Error(w, http.StatusText(500), 500)
		return
	}
	w.Write(b)
}

func jsonResponse(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

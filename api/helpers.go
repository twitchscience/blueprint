package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

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

func getAvailableSuggestions(docRoot string) ([]string, error) {
	var availableSuggestions []string
	entries, err := ioutil.ReadDir(docRoot + "/events")
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".json") {
			availableSuggestions = append(availableSuggestions, entry.Name())
		}
	}
	return availableSuggestions, nil
}

func validSuggestion(suggestion, docRoot string) bool {
	availableSuggestions, err := getAvailableSuggestions(docRoot)
	if err != nil {
		return false
	}
	for _, s := range availableSuggestions {
		if suggestion == s {
			return true
		}
	}
	return false
}

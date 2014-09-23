package api

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

type SchemaSuggestion struct {
	EventName string
	Occurred  int
}

func staticPath(root, file string) string {
	if file == "/" {
		file = "/index.html"
	}
	return path.Join(root, file)
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

func getAvailableSuggestions(docRoot string) ([]SchemaSuggestion, error) {
	var availableSuggestions []SchemaSuggestion
	entries, err := ioutil.ReadDir(path.Join(docRoot, "events"))
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".json") {
			var newSuggestion SchemaSuggestion
			f, err := os.Open(path.Join(docRoot, "events", entry.Name()))
			if err != nil {
				return nil, err
			}
			defer f.Close()

			dec := json.NewDecoder(f)

			err = dec.Decode(&newSuggestion)
			if err != nil {
				return nil, err
			}
			availableSuggestions = append(availableSuggestions, newSuggestion)
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
		if suggestion == s.EventName {
			return true
		}
	}
	return false
}

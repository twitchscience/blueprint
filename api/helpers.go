package api

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
)

// SchemaSuggestion indicates a schema for an event that has occurred a certain number of times.
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

func writeEvent(w http.ResponseWriter, events interface{}) {
	b, err := json.Marshal(events)
	if err != nil {
		log.Println("Error serializing data")
		http.Error(w, http.StatusText(500), 500)
		return
	}
	_, err = w.Write(b)
	if err != nil {
		log.Printf("Error writing json to response: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
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
			p := path.Join(docRoot, "events", entry.Name())
			f, err := os.Open(p)
			if err != nil {
				return nil, err
			}
			defer func() {
				err = f.Close()
				if err != nil {
					log.Printf("Error closing file %s: %v.", p, err)
				}
			}()

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

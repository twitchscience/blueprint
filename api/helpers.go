package api

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/twitchscience/aws_utils/logger"
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
	http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}

func writeEvent(w http.ResponseWriter, events interface{}) {
	b, err := json.Marshal(events)
	if err != nil {
		logger.WithError(err).Error("Failed to serialize data")
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	_, err = w.Write(b)
	if err != nil {
		logger.WithError(err).Error("Failed to write JSON to response")
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

func getNewSuggestion(docRoot string, name string) (newSuggestion SchemaSuggestion, err error) {
	p := path.Join(docRoot, "events", name)
	f, err := os.Open(p)
	if err != nil {
		return
	}

	defer func() {
		closeErr := f.Close()
		if closeErr != nil {
			logger.WithError(err).WithField("path", p).Error("Failed to close file")
		}
		if err == nil {
			err = closeErr
		}
	}()

	err = json.NewDecoder(f).Decode(&newSuggestion)
	return
}

func getAvailableSuggestions(docRoot string) ([]SchemaSuggestion, error) {
	var availableSuggestions []SchemaSuggestion
	entries, err := ioutil.ReadDir(path.Join(docRoot, "events"))
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			newSuggestion, err := getNewSuggestion(docRoot, entry.Name())
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

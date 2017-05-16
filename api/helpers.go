package api

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

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

func writeStructToResponse(w http.ResponseWriter, events interface{}) {
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
			logger.WithError(closeErr).WithField("path", p).Error("Failed to close file")
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

func (s *server) requestTableDeletion(schemaName string, reason string, username string) (err error) {
	v := url.Values{}
	v.Set("table", schemaName)
	v.Set("reason", reason)
	v.Set("user", username)
	url := fmt.Sprintf("%s%s", s.slackbotURL, slackbotDeletePath)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.PostForm(url, v)
	if err != nil {
		return fmt.Errorf("error making slackbot deletion request: %v", err)
	}
	defer func() {
		cerr := resp.Body.Close()
		if cerr != nil && err == nil {
			err = fmt.Errorf("failed to close slackbot deletion response body: %v", cerr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error reading slackbot deletion response body: %v", err)
		}
		return fmt.Errorf("error in slackbot (code %d): %s", resp.StatusCode, body)
	}
	return nil
}

func decodeBody(body io.ReadCloser, requestObj interface{}) error {
	defer func() {
		err := body.Close()
		if err != nil {
			logger.WithError(err).Error("Failed to close request body")
		}
	}()

	err := json.NewDecoder(body).Decode(requestObj)
	if err != nil {
		return fmt.Errorf("decoding json: %v", err)
	}
	return nil
}

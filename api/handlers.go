package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/blueprint/bpdb"
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/scoop_protocol/transformer"

	"github.com/zenazn/goji/web"
)

// respondWithJSONError responds with a JSON error with the given error code. The format of the
// JSON error is {"Error": text}
// It's very likely that you want to return from the handler after calling
// this.
func respondWithJSONError(w http.ResponseWriter, text string, responseCode int) {

	var jsonError struct {
		Error string
	}
	jsonError.Error = text
	js, err := json.Marshal(jsonError)
	if err != nil {
		logger.WithError(err).Error("Failed to marshal JSON")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(responseCode)
	_, err = w.Write(js)
	if err != nil {
		logger.WithError(err).Error("Failed to write JSON to response")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ingest proxies the request through to the ingester /control/ingest
func (s *server) ingest(c web.C, w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var tableArg struct {
		Table string
	}
	err := decoder.Decode(&tableArg)
	if err != nil {
		respondWithJSONError(w, "Problem decoding JSON POST data.", http.StatusBadRequest)
		return
	}

	fields := map[string]interface{}{"table": tableArg.Table}
	fields["user_requesting"] = c.Env["username"].(string)
	logger.WithFields(fields).Info("Table flush request")

	js, err := json.Marshal(tableArg)
	if err != nil {
		logger.WithError(err).Error("Failed to marshal JSON")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	req, err := http.NewRequest("POST", ingesterURL+"/control/ingest", bytes.NewBuffer(js))
	if err != nil {
		respondWithJSONError(w, "Error building request to ingester: "+err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		respondWithJSONError(w, "Error making request to ingester: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logger.WithError(err).Error("Failed to close response body")
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	if err != nil {
		logger.WithError(err).Error("Failed to read from response")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = w.Write(buf.Bytes())
	if err != nil {
		logger.WithError(err).Error("Failed to write to response")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	return
}

func (s *server) createSchema(c web.C, w http.ResponseWriter, r *http.Request) {
	defer func() {
		err := r.Body.Close()
		if err != nil {
			logger.WithError(err).Error("Failed to close request body")
		}
	}()

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logger.WithError(err).Error("Error reading body of request in createSchema")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var cfg scoop_protocol.Config
	err = json.Unmarshal(b, &cfg)
	if err != nil {
		logger.WithError(err).Error("Error getting marshalling config to json")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	blacklisted, err := s.isBlacklisted(cfg.EventName)
	if err != nil {
		logger.WithError(err).
			WithField("event_name", cfg.EventName).
			Error("Failed to test event in the blacklist")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if blacklisted {
		http.Error(w, fmt.Sprintf("%v is blacklisted", cfg.EventName), http.StatusForbidden)
		return
	}

	err = s.bpdbBackend.CreateSchema(&cfg, c.Env["username"].(string))
	if err != nil {
		logger.WithError(err).Error("Error creating schema.")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

var (
	blacklistOnce sync.Once
	blacklistRe   []*regexp.Regexp
	blacklistErr  error
)

// isBlacklisted check whether name matches any regex in the blacklist (case insensitive).
// It returns false when name is not blacklisted or an error occurs.
// TODO(clgroft): should this be per-server? Currently it's global.
func (s *server) isBlacklisted(name string) (bool, error) {
	blacklistOnce.Do(func() {
		var configJSON []byte
		configJSON, blacklistErr = ioutil.ReadFile(s.configFilename)
		if blacklistErr != nil {
			return
		}

		var jsonObj map[string][]string
		blacklistErr = json.Unmarshal(configJSON, &jsonObj)
		if blacklistErr != nil {
			return
		}

		blacklist, ok := jsonObj["blacklist"]
		if !ok {
			blacklistErr = fmt.Errorf("Cannot find blacklist in %v", s.configFilename)
			return
		}

		for _, pattern := range blacklist {
			var re *regexp.Regexp
			re, blacklistErr = regexp.Compile(strings.ToLower(pattern))
			if blacklistErr != nil {
				blacklistRe = nil
				return
			}
			blacklistRe = append(blacklistRe, re)
		}
	})

	if blacklistErr != nil {
		return false, blacklistErr
	}

	name = strings.ToLower(name)

	for _, pattern := range blacklistRe {
		if pattern.MatchString(name) {
			return true, nil
		}
	}
	return false, nil
}

func (s *server) updateSchema(c web.C, w http.ResponseWriter, r *http.Request) {
	eventName := c.URLParams["id"]

	defer func() {
		err := r.Body.Close()
		if err != nil {
			logger.WithError(err).Error("Failed to close request body")
		}
	}()

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logger.WithError(err).Error("Error reading request body in updateSchema")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var req core.ClientUpdateSchemaRequest
	err = json.Unmarshal(b, &req)
	if err != nil {
		logger.WithError(err).Error("Error unmarshalling request body in updateSchema")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req.EventName = eventName

	err = s.bpdbBackend.UpdateSchema(&req, c.Env["username"].(string))
	if err != nil {
		logger.WithError(err).Error("Error updating schema.")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) allSchemas(w http.ResponseWriter, r *http.Request) {
	schemas, err := s.bpdbBackend.AllSchemas()
	if err != nil {
		logger.WithError(err).Error("Error retrieving allSchemas")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeEvent(w, schemas)
}

func (s *server) schema(c web.C, w http.ResponseWriter, r *http.Request) {
	schema, err := s.bpdbBackend.Schema(c.URLParams["id"])
	if err != nil {
		logger.WithError(err).WithField("schema", c.URLParams["id"]).Error("Error retrieving schema")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if schema == nil {
		fourOhFour(w, r)
		return
	}
	writeEvent(w, []*bpdb.AnnotatedSchema{schema})
}

func (s *server) migration(c web.C, w http.ResponseWriter, r *http.Request) {
	args := r.URL.Query()
	to, err := strconv.Atoi(args.Get("to_version"))
	if err != nil || to < 0 {
		respondWithJSONError(w, "Error, 'to_version' argument must be non-negative integer.", http.StatusBadRequest)
		logger.WithError(err).
			WithField("to_version", args.Get("to_version")).
			Error("'to_version' must be non-negative integer")
		return
	}
	operations, err := s.bpdbBackend.Migration(
		c.URLParams["schema"],
		to,
	)
	if err != nil {
		respondWithJSONError(w, "Internal Service Error", http.StatusInternalServerError)
		logger.WithError(err).Error("Failed to get migration steps")
		return
	}
	if len(operations) == 0 {
		respondWithJSONError(w, fmt.Sprintf("No migration for table '%s' to v%d.", c.URLParams["schema"], to), http.StatusBadRequest)
		return
	}
	b, err := json.Marshal(operations)
	if err != nil {
		logger.WithError(err).Error("Error getting marshalling operations to json")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = w.Write(b)
	if err != nil {
		logger.WithError(err).Error("Failed to write to response")
		respondWithJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) fileHandler(w http.ResponseWriter, r *http.Request) {
	fname := staticPath(s.docRoot, r.URL.Path)
	fh, err := os.Open(fname)
	if err != nil {
		fourOhFour(w, r)
		return
	}
	_, err = io.Copy(w, fh)
	if err != nil {
		logger.WithError(err).
			WithField("filename", fname).
			Error("Failed to copy file to response")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *server) types(w http.ResponseWriter, r *http.Request) {
	data := make(map[string][]string)
	data["result"] = transformer.ValidTransforms
	b, err := json.Marshal(data)
	if err != nil {
		logger.WithError(err).Error("Error getting marshalling data to json")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = w.Write(b)
	if err != nil {
		logger.WithError(err).Error("Failed to write to response")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) listSuggestions(w http.ResponseWriter, r *http.Request) {
	availableSuggestions, err := getAvailableSuggestions(s.docRoot)
	if err != nil {
		logger.WithError(err).Error("Error listing suggestions")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(availableSuggestions) == 0 {
		_, err = w.Write([]byte("[]"))
		if err != nil {
			logger.WithError(err).Error("Failed to write to response")
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	b, err := json.Marshal(availableSuggestions)
	if err != nil {
		logger.WithError(err).Error("Error getting marshalling suggestions to json")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = w.Write(b)
	if err != nil {
		logger.WithError(err).Error("Failed to write to response")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) suggestion(c web.C, w http.ResponseWriter, r *http.Request) {
	if !validSuggestion(strings.TrimSuffix(c.URLParams["id"], ".json"), s.docRoot) {
		fourOhFour(w, r)
		return
	}
	fname := path.Join(s.docRoot, "events", c.URLParams["id"])
	fh, err := os.Open(fname)
	if err != nil {
		fourOhFour(w, r)
		return
	}
	_, err = io.Copy(w, fh)
	if err != nil {
		logger.WithError(err).WithField("filename", fname).Error("Failed to copy file to response")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *server) removeSuggestion(c web.C, w http.ResponseWriter, r *http.Request) {
	if !validSuggestion(strings.TrimSuffix(c.URLParams["id"], ".json"), s.docRoot) {
		fourOhFour(w, r)
		return
	}

	err := os.Remove(s.docRoot + "/events/" + c.URLParams["id"])
	if err != nil {
		fourOhFour(w, r)
		return
	}
}

func (s *server) healthCheck(c web.C, w http.ResponseWriter, r *http.Request) {
	_, err := io.WriteString(w, "Healthy")
	if err != nil {
		logger.WithError(err).Error("Failed to write to response")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

package api

import (
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
	"time"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/blueprint/bpdb"
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/scoop_protocol/transformer"

	"github.com/zenazn/goji/web"
)

type config struct {
	CacheTimeoutSecs time.Duration
	Blacklist        []string
}

func (s *server) loadConfig() error {
	configJSON, err := ioutil.ReadFile(s.configFilename)
	if err != nil {
		return err
	}

	var jsonObj config
	if err := json.Unmarshal(configJSON, &jsonObj); err != nil {
		return err
	}
	s.cacheTimeout = jsonObj.CacheTimeoutSecs * time.Second
	blacklist := jsonObj.Blacklist

	for _, pattern := range blacklist {
		re, err := regexp.Compile(strings.ToLower(pattern))
		if err != nil {
			return err
		}
		s.blacklistRe = append(s.blacklistRe, re)
	}
	return nil
}

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

	err = s.ingesterController.ForceIngest(tableArg.Table)
	if err != nil {
		logger.WithError(err).Error("Failed to issue ForceLoad to ingester")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
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

	if s.isBlacklisted(cfg.EventName) {
		http.Error(w, fmt.Sprintf("%v is blacklisted", cfg.EventName), http.StatusForbidden)
		return
	}

	defer s.clearCache()
	err = s.bpdbBackend.CreateSchema(&cfg, c.Env["username"].(string))
	if err != nil {
		logger.WithError(err).Error("Error creating schema.")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// isBlacklisted check whether name matches any regex in the blacklist (case insensitive).
// It returns false when name is not blacklisted or an error occurs.
func (s *server) isBlacklisted(name string) bool {
	name = strings.ToLower(name)
	for _, pattern := range s.blacklistRe {
		if pattern.MatchString(name) {
			return true
		}
	}
	return false
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

	defer s.clearCache()
	requestErr, serverErr := s.bpdbBackend.UpdateSchema(&req, c.Env["username"].(string))
	if serverErr != nil {
		logger.WithError(serverErr).Error("Error updating schema")
		http.Error(w, serverErr.Error(), http.StatusInternalServerError)
		return
	}
	if requestErr != "" {
		logger.WithField("requestErr", requestErr).Warn("Error in updateSchema request")
		http.Error(w, requestErr, http.StatusBadRequest)
		return
	}
}

func (s *server) dropSchema(c web.C, w http.ResponseWriter, r *http.Request) {
	defer func() {
		err := r.Body.Close()
		if err != nil {
			logger.WithError(err).Error("Failed to close request body")
		}
	}()

	var req core.ClientDropSchemaRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		logger.WithError(err).Warn("Error decoding request body in dropSchema")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	schema, err := s.bpdbBackend.Schema(req.EventName)
	if err != nil {
		logger.WithError(err).WithField("schema", req.EventName).Error("Error retrieving schema")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if schema == nil {
		http.Error(w, "Unknown schema", http.StatusBadRequest)
		return
	}

	exists, err := s.ingesterController.TableExists(schema.EventName)
	if err != nil {
		logger.WithError(err).Error("Error determining if schema exists")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if exists {
		err = s.requestTableDeletion(schema.EventName, req.Reason, c.Env["username"].(string))
		if err != nil {
			logger.WithError(err).Error("Error making slackbot deletion request")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		err = s.ingesterController.IncrementVersion(schema.EventName)
		if err != nil {
			logger.WithError(err).Error("Error incrementing version in ingester")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	defer s.clearCache()
	err = s.bpdbBackend.DropSchema(schema, req.Reason, exists, c.Env["username"].(string))
	if err != nil {
		logger.WithError(err).Error("Error dropping schema in operation")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func (s *server) allSchemas(w http.ResponseWriter, r *http.Request) {
	result := s.getAllSchemas()
	if result.err != nil {
		http.Error(w, result.err.Error(), http.StatusInternalServerError)
	} else {
		writeEvent(w, result.allSchemas)
	}
}

func (s *server) clearCache() {
	s.cacheSynchronizer <- func() { s.cachedResult = nil }
}

func (s *server) timeoutCache(oldVersion int) {
	logger.Go(func() {
		time.Sleep(s.cacheTimeout)
		s.cacheSynchronizer <- func() {
			if oldVersion == s.cachedVersion {
				s.cachedResult = nil
			}
		}
	})
}

func (s *server) getAllSchemas() *schemaResult {
	resultChan := make(chan *schemaResult)
	s.cacheSynchronizer <- func() {
		if s.cachedResult != nil {
			resultChan <- s.cachedResult
			return
		}

		schemas, err := s.bpdbBackend.AllSchemas()
		if err != nil {
			logger.WithError(err).Error("Failed to retrieve all schemas")
		}
		s.cachedVersion++
		s.cachedResult = &schemaResult{schemas, err}
		s.timeoutCache(s.cachedVersion)

		resultChan <- s.cachedResult
	}
	return <-resultChan
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

func respondWithJSONBool(w http.ResponseWriter, key string, result bool) {
	js, err := json.Marshal(map[string]bool{key: result})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(js)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// TODO: Update goji to goji/goji so handlers with URLParams are testable.
func (s *server) droppableSchema(c web.C, w http.ResponseWriter, r *http.Request) {
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
	exists, err := s.ingesterController.TableExists(schema.EventName)
	if err != nil {
		logger.WithError(err).Error("Error determining if schema exists")
		// default to true so we don't break the page.
		exists = true
	}

	respondWithJSONBool(w, "Droppable", !exists)
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

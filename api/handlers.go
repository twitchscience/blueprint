package api

import (
	"encoding/json"
	"errors"
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

	"github.com/sirupsen/logrus"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/blueprint/bpdb"
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/blueprint/ingester"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/scoop_protocol/transformer"

	"github.com/zenazn/goji/web"
)

type config struct {
	CacheTimeoutSecs time.Duration
	Blacklist        []string
}

type maintenanceMode struct {
	IsMaintenance bool   `json:"is_maintenance"`
	Reason        string `json:"reason"`
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

// maintenanceHandler sends an error if Blueprint is in maintenance mode and otherwise yields to
// the given http.Handler.
func (s *server) maintenanceHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.WithFields(map[string]interface{}{
			"http_method": r.Method,
			"url":         r.URL,
		}).Info("Maintenance middleware invoked")
		isInMaintenanceMode := s.bpdbBackend.IsInMaintenanceMode()
		logger.WithField("is_maintenance", isInMaintenanceMode).Info("Checked maintenance mode")
		if isInMaintenanceMode {
			respondWithJSONError(
				w,
				"Blueprint is in maintenance mode; no modifications are allowed",
				http.StatusServiceUnavailable)
			return
		}

		h.ServeHTTP(w, r)
	})
}

// forceLoad proxies the request through to the ingester
func (s *server) forceLoad(c web.C, w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var tableArg struct {
		Table string
	}
	err := decoder.Decode(&tableArg)
	if err != nil {
		respondWithJSONError(w, "Problem decoding JSON POST data.", http.StatusBadRequest)
		return
	}

	requester := c.Env["username"].(string)
	logger.WithFields(logrus.Fields{
		"table":     tableArg.Table,
		"requester": requester,
	}).Info("Table flush request")

	err = s.ingesterController.ForceLoad(tableArg.Table, requester)
	if err != nil {
		logger.WithError(err).Error("Failed to issue ForceLoad to ingester")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) createSchema(c web.C, w http.ResponseWriter, r *http.Request) {
	webErr := s.createSchemaHelper(c.Env["username"].(string), r.Body)
	if webErr != nil {
		webErr.ReportError(w, "Error creating schema")
	}
}

func (s *server) createSchemaHelper(username string, body io.ReadCloser) *core.WebError {
	var cfg scoop_protocol.Config
	err := decodeBody(body, &cfg)
	if err != nil {
		return core.NewServerWebError(err)
	}

	if s.isBlacklisted(cfg.EventName) {
		return core.NewUserWebErrorf("%s is blacklisted", cfg.EventName)
	}

	defer s.clearCache()
	return s.bpSchemaBackend.CreateSchema(&cfg, username)
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

	webErr := s.updateSchemaHelper(eventName, c.Env["username"].(string), r.Body)
	if webErr != nil {
		webErr.ReportError(w, "Error updating schema")
	}
}

func (s *server) updateSchemaHelper(eventName string, username string, body io.ReadCloser) *core.WebError {
	var req core.ClientUpdateSchemaRequest
	err := decodeBody(body, &req)
	if err != nil {
		return core.NewServerWebError(err)
	}
	req.EventName = eventName

	defer s.clearCache()
	return s.bpSchemaBackend.UpdateSchema(&req, username)
}

func (s *server) dropSchema(c web.C, w http.ResponseWriter, r *http.Request) {
	webErr := s.dropSchemaHelper(c.Env["username"].(string), r.Body)
	if webErr != nil {
		webErr.ReportError(w, "Error dropping schema")
	}
}

func (s *server) dropSchemaHelper(username string, body io.ReadCloser) *core.WebError {
	var req core.ClientDropSchemaRequest
	err := decodeBody(body, &req)
	if err != nil {
		return core.NewServerWebError(err)
	}

	schema, err := s.bpSchemaBackend.Schema(req.EventName)
	if err != nil {
		return core.NewServerWebErrorf("retrieving schema: %v", err)
	}
	if schema == nil {
		return core.NewUserWebErrorf("unknown schema")
	}

	exists, err := s.ingesterController.TableExists(schema.EventName)
	if err != nil {
		return core.NewServerWebErrorf("determining if schema exists: %v", err)
	}

	if exists {
		err = s.requestTableDeletion(schema.EventName, req.Reason, username)
		if err != nil {
			return core.NewServerWebErrorf("making slackbot deletion request: %v", err)
		}
	} else {
		err = s.ingesterController.IncrementVersion(schema.EventName)
		if err != nil {
			return core.NewServerWebErrorf("incrementing version in ingester: %v", err)
		}
	}

	defer s.clearCache()
	err = s.bpSchemaBackend.DropSchema(schema, req.Reason, exists, username)
	if err != nil {
		return core.NewServerWebErrorf("dropping schema in operation table: %v", err)
	}
	return nil

}

func (s *server) allSchemas(w http.ResponseWriter, r *http.Request) {
	result := s.getAllSchemas()
	if result.err != nil {
		http.Error(w, result.err.Error(), http.StatusInternalServerError)
	} else {
		writeStructToResponse(w, result.allSchemas)
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

		schemas, err := s.bpSchemaBackend.AllSchemas()
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
	schema, err := s.bpSchemaBackend.Schema(c.URLParams["id"])
	if err != nil {
		logger.WithError(err).WithField("schema", c.URLParams["id"]).Error("Error retrieving schema")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if schema == nil {
		fourOhFour(w, r)
		return
	}
	writeStructToResponse(w, []*bpdb.AnnotatedSchema{schema})
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
	schema, err := s.bpSchemaBackend.Schema(c.URLParams["id"])
	if err != nil {
		logger.WithError(err).WithField("schema", c.URLParams["id"]).Error("Error retrieving schema")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if schema == nil {
		fourOhFour(w, r)
		return
	}
	if time.Since(schema.CreatedTS) < time.Second*3600 {
		respondWithJSONBool(w, "Droppable", false)
		return
	}
	exists, err := s.ingesterController.TableExists(schema.EventName)
	if err != nil {
		if _, ok := err.(ingester.ServiceUnavailableError); ok {
			logger.Warn("Ingester is currently unavailable")
		} else {
			logger.WithError(err).Error("Error determining if schema exists")
		}
		// default to true so we don't break the page.
		exists = true
	}

	respondWithJSONBool(w, "Droppable", !exists)
}

func (s *server) eventComment(c web.C, w http.ResponseWriter, r *http.Request) {
	schema, err := s.bpSchemaBackend.Schema(c.URLParams["event"])
	if err != nil {
		logger.WithError(err).WithField("schema", c.URLParams["event"]).Error("Error retrieving schema")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if schema == nil {
		fourOhFour(w, r)
		return
	}
	eventComment, err := s.bpEventCommentBackend.EventComment(c.URLParams["event"])
	if err != nil {
		logger.WithError(err).WithField("eventComment", c.URLParams["event"]).Error("Error retrieving event comment")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeStructToResponse(w, []*bpdb.EventComment{eventComment})
}

func (s *server) updateEventComment(c web.C, w http.ResponseWriter, r *http.Request) {
	eventName := c.URLParams["event"]
	webErr := s.updateEventCommentHelper(eventName, c.Env["username"].(string), r.Body)
	if webErr != nil {
		webErr.ReportError(w, "Error updating event comment")
	}
}

func (s *server) updateEventCommentHelper(eventName string, username string, body io.ReadCloser) *core.WebError {
	var req core.ClientUpdateEventCommentRequest
	err := decodeBody(body, &req)
	if err != nil {
		return core.NewServerWebError(err)
	}
	req.EventName = eventName

	return s.bpEventCommentBackend.UpdateEventComment(&req, username)
}

func (s *server) eventMetadata(c web.C, w http.ResponseWriter, r *http.Request) {
	schema, err := s.bpSchemaBackend.Schema(c.URLParams["event"])
	if err != nil {
		logger.WithError(err).WithField("schema", c.URLParams["event"]).Error("Error retrieving schema")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if schema == nil {
		fourOhFour(w, r)
		return
	}
	eventMetadata, err := s.bpEventMetadataBackend.EventMetadata(c.URLParams["event"])
	if err != nil {
		logger.WithError(err).WithField("eventMetadata", c.URLParams["event"]).Error("Error retrieving event metadata")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeStructToResponse(w, []*bpdb.EventMetadata{eventMetadata})
}

func (s *server) updateEventMetadata(c web.C, w http.ResponseWriter, r *http.Request) {
	eventName := c.URLParams["event"]
	var req core.ClientUpdateEventMetadataRequest
	err := decodeBody(r.Body, &req)
	if err != nil {
		core.NewServerWebError(err).ReportError(w, "Error decoding request body")
		return
	}

	req.EventName = eventName
	if req.MetadataType != scoop_protocol.COMMENT && req.MetadataType != scoop_protocol.EDGE_TYPE {
		err = errors.New("Invalid event metadata type")
		core.NewServerWebError(err).ReportError(w, "Invalid event metadata type")
		return
	}

	webErr := s.bpEventMetadataBackend.UpdateEventMetadata(&req, c.Env["username"].(string))
	if webErr != nil {
		webErr.ReportError(w, "Error updating event metadata")
	}
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
	operations, err := s.bpSchemaBackend.Migration(
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
	err := s.listSuggestionsHelper(w)
	if err != nil {
		logger.WithError(err).Error("Error listing suggestions")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *server) listSuggestionsHelper(w io.Writer) error {
	availableSuggestions, err := getAvailableSuggestions(s.docRoot)
	if err != nil {
		return fmt.Errorf("getting suggestions: %v", err)
	}

	if len(availableSuggestions) == 0 {
		_, err = w.Write([]byte("[]"))
		if err != nil {
			return fmt.Errorf("writing empty response: %v", err)
		}
		return nil
	}

	b, err := json.Marshal(availableSuggestions)
	if err != nil {
		return fmt.Errorf("marshalling suggestions to json: %v", err)
	}

	_, err = w.Write(b)
	if err != nil {
		return fmt.Errorf("writing response: %v", err)
	}
	return nil
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

func (s *server) getMaintenanceMode(w http.ResponseWriter, r *http.Request) {
	isInMaintenanceMode := s.bpdbBackend.IsInMaintenanceMode()
	logger.WithField("is_maintenance", isInMaintenanceMode).Info("Serving GetMaintenanceMode request")
	respondWithJSONBool(w, "is_maintenance", isInMaintenanceMode)
}

func (s *server) setMaintenanceMode(w http.ResponseWriter, r *http.Request) {
	webErr := s.setMaintenanceModeHelper(r.Body)
	if webErr != nil {
		webErr.ReportError(w, "Error setting maintenance mode")
	}

}

func (s *server) setMaintenanceModeHelper(body io.ReadCloser) *core.WebError {
	var mm maintenanceMode
	err := decodeBody(body, &mm)
	if err != nil {
		return core.NewServerWebError(err)
	}

	if err = s.bpdbBackend.SetMaintenanceMode(mm.IsMaintenance, mm.Reason); err != nil {
		return core.NewServerWebErrorf("setting maintenance mode: %v", err)
	}
	logger.WithField("is_maintenance", mm.IsMaintenance).WithField("reason", mm.Reason).Info("Maintenance mode set")
	return nil
}

func (s *server) healthCheck(c web.C, w http.ResponseWriter, r *http.Request) {
	_, err := io.WriteString(w, "Healthy")
	if err != nil {
		logger.WithError(err).Error("Failed to write to response")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *server) stats(w http.ResponseWriter, r *http.Request) {
	err := s.statsHelper(w)
	if err != nil {
		logger.WithError(err).Error("Error getting stats")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *server) statsHelper(w io.Writer) error {
	dailyChanges, err := s.bpdbBackend.DailyChangesLast30Days()
	if err != nil {
		return fmt.Errorf("getting daily changes from bpdb: %v", err)
	}

	activeUsers, err := s.bpdbBackend.ActiveUsersLast30Days()
	if err != nil {
		return fmt.Errorf("getting active users from bpdb: %v", err)
	}

	b, err := json.Marshal(&struct {
		DailyChanges []*bpdb.DailyChange
		ActiveUsers  []*bpdb.ActiveUser
	}{DailyChanges: dailyChanges, ActiveUsers: activeUsers})
	if err != nil {
		return fmt.Errorf("marshalling stats to json: %v", err)
	}

	_, err = w.Write(b)
	if err != nil {
		return fmt.Errorf("writing response: %v", err)
	}
	return nil
}

func (s *server) allKinesisConfigs(w http.ResponseWriter, r *http.Request) {
	schemas, err := s.bpKinesisConfigBackend.AllKinesisConfigs()
	if err != nil {
		reportKinesisConfigServerError(w, err, "Failed to retrieve all Kinesis configs")
	}
	writeStructToResponse(w, schemas)
}

func reportKinesisConfigUserError(w http.ResponseWriter, err error, msg string) {
	webErr := core.NewUserWebError(err)
	webErr.ReportError(w, msg)
}

func reportKinesisConfigServerError(w http.ResponseWriter, err error, msg string) {
	webErr := core.NewServerWebError(err)
	webErr.ReportError(w, msg)
}

func (s *server) kinesisconfig(c web.C, w http.ResponseWriter, r *http.Request) {
	accountNumber, err := strconv.ParseInt(c.URLParams["account"], 10, 64)
	if err != nil {
		reportKinesisConfigUserError(w, err, "Non-numeric account number supplied.")
		return
	}
	config, err := s.bpKinesisConfigBackend.KinesisConfig(accountNumber, c.URLParams["type"], c.URLParams["name"])
	if err != nil {
		reportKinesisConfigServerError(w, err, "Error retrieving Kinesis config")
		return
	}
	if config == nil {
		fourOhFour(w, r)
		return
	}
	writeStructToResponse(w, config)
}

func (s *server) updateKinesisConfig(c web.C, w http.ResponseWriter, r *http.Request) {
	accountNumber, err := strconv.ParseInt(c.URLParams["account"], 10, 64)
	if err != nil {
		reportKinesisConfigUserError(w, err, "Non-numeric account number supplied.")
		return
	}
	streamType := c.URLParams["type"]
	streamName := c.URLParams["name"]
	webErr := s.updateKinesisConfigHelper(accountNumber, streamType, streamName, c.Env["username"].(string), r.Body)
	if webErr != nil {
		webErr.ReportError(w, "Error updating Kinesis config")
	}
}

func (s *server) updateKinesisConfigHelper(account int64, streamType string, streamName string, username string, body io.ReadCloser) *core.WebError {
	var req struct {
		Kinesisconfig scoop_protocol.AnnotatedKinesisConfig
	}
	err := decodeBody(body, &req)
	if err != nil {
		return core.NewUserWebError(err)
	}

	return s.bpKinesisConfigBackend.UpdateKinesisConfig(&req.Kinesisconfig, username)
}

func (s *server) createKinesisConfig(c web.C, w http.ResponseWriter, r *http.Request) {
	webErr := s.createKinesisConfigHelper(c.Env["username"].(string), r.Body)
	if webErr != nil {
		webErr.ReportError(w, "Error creating Kinesis config")
	}
}

func (s *server) createKinesisConfigHelper(username string, body io.ReadCloser) *core.WebError {
	var config scoop_protocol.AnnotatedKinesisConfig
	err := decodeBody(body, &config)
	if err != nil {
		return core.NewUserWebError(err)
	}
	return s.bpKinesisConfigBackend.CreateKinesisConfig(&config, username)
}

func (s *server) dropKinesisConfig(c web.C, w http.ResponseWriter, r *http.Request) {
	webErr := s.dropKinesisConfigHelper(c.Env["username"].(string), r.Body)
	if webErr != nil {
		webErr.ReportError(w, "Error dropping schema")
	}
}

func (s *server) dropKinesisConfigHelper(username string, body io.ReadCloser) *core.WebError {
	var req struct {
		StreamName string
		StreamType string
		AWSAccount int64
		Reason     string
	}
	err := decodeBody(body, &req)
	if err != nil {
		return core.NewUserWebError(err)
	}

	current, err := s.bpKinesisConfigBackend.KinesisConfig(req.AWSAccount, req.StreamType, req.StreamName)
	if err != nil {
		return core.NewServerWebErrorf("retrieving Kinesis config: %v", err)
	}
	if current == nil {
		return core.NewUserWebErrorf("unknown Kinesis config to drop")
	}

	err = s.bpKinesisConfigBackend.DropKinesisConfig(current, req.Reason, username)
	if err != nil {
		return core.NewServerWebErrorf("dropping Kinesis config in bpdb table: %v", err)
	}
	return nil
}

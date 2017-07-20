package api

import (
	"bytes"
	"compress/gzip"
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

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/sirupsen/logrus"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/blueprint/bpdb"
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/blueprint/ingester"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/scoop_protocol/transformer"

	"github.com/zenazn/goji/web"
)

const (
	schemaConfigS3Key        = "schema-configs.json.gz"
	kinesisConfigS3Key       = "kinesis-configs.json.gz"
	eventMetadataConfigS3Key = "event-metadata-configs.json.gz"
)

type config struct {
	CacheTimeoutSecs      time.Duration
	S3BpConfigsBucketName string
	S3BpConfigsPrefix     string
	Blacklist             []string
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
	s.s3BpConfigsBucketName = jsonObj.S3BpConfigsBucketName
	s.s3BpConfigsPrefix = jsonObj.S3BpConfigsPrefix
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

// maintenanceHandler sends an error if Blueprint is in global maintenance mode and otherwise
// yields to the given http.Handler.
func (s *server) maintenanceHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.WithFields(map[string]interface{}{
			"http_method": r.Method,
			"url":         r.URL,
		}).Info("Maintenance middleware invoked")
		mm := s.bpdbBackend.GetMaintenanceMode()
		logger.WithField("is_maintenance", mm.IsInMaintenanceMode).Info("Checked maintenance mode")
		if mm.IsInMaintenanceMode {
			respondWithJSONError(
				w,
				"Blueprint is in maintenance mode; no modifications are allowed",
				http.StatusServiceUnavailable)
			return
		}

		h.ServeHTTP(w, r)
	})
}

// maintenanceModeGuard writes a 503 error if the given schema is in maintenance mode
// and returns true, otherwise just returns false
func (s *server) maintenanceModeGuard(schema string, w http.ResponseWriter) bool {
	mm := s.bpdbBackend.GetSchemaMaintenanceMode(schema)
	if mm.IsInMaintenanceMode {
		respondWithJSONError(
			w,
			fmt.Sprintf("Schema %s is in maintenance mode; no modifications are allowed", schema),
			http.StatusServiceUnavailable)
	}
	return mm.IsInMaintenanceMode
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
		logger.WithError(err).Warning("Failed to issue ForceLoad to ingester")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// publishToS3Helper uploads configs to the appropriate s3 bucket
func publishToS3Helper(svc s3manageriface.UploaderAPI, configs interface{}, bucket string, configS3Key string) error {
	if bucket == "" {
		return errors.New("No bucket name specified for publishing to S3")
	}

	b, err := json.Marshal(configs)
	if err != nil {
		return err
	}

	var compressedBytes bytes.Buffer
	w, err := gzip.NewWriterLevel(&compressedBytes, gzip.BestCompression)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}

	uploadParams := &s3manager.UploadInput{
		Bucket: &bucket,
		Key:    &configS3Key,
		Body:   bytes.NewReader(compressedBytes.Bytes()),
	}

	result, err := svc.Upload(uploadParams)
	if err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("Published %s to S3 location %s and uploadID %s", configS3Key, result.Location, result.UploadID))
	return nil
}

/* publishToS3 calls publishToS3Helper and checks if it was successful
   We are not returning an error since up until the point in the calling function
   where publishToS3 is called, no other errors have occurred, so the requester
   should get the data independent of whether the S3 upload succeeded.
   Hence, failed attempts to upload to S3 won't inadvertently break Blueprint */
func publishToS3(svc s3manageriface.UploaderAPI, configs interface{}, bucket string, baseFileName string, filePrefix string) {
	configS3Key, err := getS3ConfigsFileName(baseFileName, filePrefix)
	if err != nil {
		logger.WithError(err).Errorf("Failed to publish %s to S3", configS3Key)
	}
	err = publishToS3Helper(svc, configs, bucket, configS3Key)
	if err != nil {
		logger.WithError(err).Errorf("Failed to publish %s to S3", configS3Key)
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

	defer s.goCache.Delete(allSchemasCache)
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
	if s.maintenanceModeGuard(eventName, w) {
		return // error written by maintenanceModeGuard
	}

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

	defer s.goCache.Delete(allSchemasCache)
	return s.bpSchemaBackend.UpdateSchema(&req, username)
}

func (s *server) dropSchema(c web.C, w http.ResponseWriter, r *http.Request) {
	username := c.Env["username"].(string)
	var req core.ClientDropSchemaRequest
	err := decodeBody(r.Body, &req)
	if err != nil {
		core.NewServerWebError(err).ReportError(w, "decoding drop schema request")
		return
	}

	if s.maintenanceModeGuard(req.EventName, w) {
		return // error written by maintenanceModeGuard
	}

	schema, err := s.bpSchemaBackend.Schema(req.EventName)
	if err != nil {
		core.NewServerWebError(err).ReportError(w, "retrieving schema")
		return
	}
	if schema == nil {
		core.NewUserWebErrorf("unknown schema")
		return
	}

	exists, err := s.ingesterController.TableExists(schema.EventName)
	if err != nil {
		core.NewServerWebError(err).ReportError(w, "determining if schema exists")
		return
	}

	if exists {
		err = s.requestTableDeletion(schema.EventName, req.Reason, username)
		if err != nil {
			core.NewServerWebError(err).ReportError(w, "making slackbot deletion request")
			return
		}
	} else {
		err = s.ingesterController.IncrementVersion(schema.EventName)
		if err != nil {
			core.NewServerWebError(err).ReportError(w, "incrementing version in ingester")
			return
		}
	}

	defer s.goCache.Delete(allSchemasCache)
	err = s.bpSchemaBackend.DropSchema(schema, req.Reason, exists, username)
	if err != nil {
		core.NewServerWebError(err).ReportError(w, "dropping schema in operation table")
		return
	}
}

func (s *server) allSchemas(w http.ResponseWriter, r *http.Request) {
	cachedSchemas, found := s.goCache.Get(allSchemasCache)
	if found {
		writeStructToResponse(w, cachedSchemas.([]bpdb.AnnotatedSchema))
		return
	}

	schemas, err := s.bpSchemaBackend.AllSchemas()
	if err != nil {
		logger.WithError(err).Error("Failed to retrieve all schemas")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.goCache.Set(allSchemasCache, schemas, s.cacheTimeout)
	publishToS3(s.s3Uploader, schemas, s.s3BpConfigsBucketName, schemaConfigS3Key, s.s3BpConfigsPrefix)
	writeStructToResponse(w, schemas)
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

func (s *server) allEventMetadata(w http.ResponseWriter, r *http.Request) {
	cachedMetadata, found := s.goCache.Get(allMetadataCache)
	if found {
		writeStructToResponse(w, cachedMetadata)
		return
	}

	allMetadata, err := s.bpEventMetadataBackend.AllEventMetadata()
	if err != nil {
		logger.WithError(err).Error("Failed to retrieve all metadata")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	metadata := allMetadata.Metadata
	s.goCache.Set(allMetadataCache, metadata, s.cacheTimeout)
	publishToS3(s.s3Uploader, metadata, s.s3BpConfigsBucketName, eventMetadataConfigS3Key, s.s3BpConfigsPrefix)
	writeStructToResponse(w, metadata)
}

func (s *server) eventMetadata(c web.C, w http.ResponseWriter, r *http.Request) {
	eventName := c.URLParams["event"]
	cachedEventMetadata, foundCache := s.goCache.Get(allMetadataCache)
	ret := bpdb.EventMetadata{EventName: eventName}
	if foundCache {
		metadata, exists := cachedEventMetadata.(map[string](map[string]bpdb.EventMetadataRow))[eventName]
		if exists {
			ret.Metadata = metadata
		} else {
			ret.Metadata = map[string]bpdb.EventMetadataRow{}
		}
		writeStructToResponse(w, ret)
		return
	}

	schema, err := s.bpSchemaBackend.Schema(eventName)
	if err != nil {
		logger.WithError(err).WithField("schema", eventName).Error("Error retrieving schema")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if schema == nil {
		fourOhFour(w, r)
		return
	}

	allMetadata, err := s.bpEventMetadataBackend.AllEventMetadata()
	if err != nil {
		logger.WithError(err).Error("Failed to retrieve all metadata")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	metadata := allMetadata.Metadata
	s.goCache.Set(allMetadataCache, metadata, s.cacheTimeout)
	publishToS3(s.s3Uploader, metadata, s.s3BpConfigsBucketName, eventMetadataConfigS3Key, s.s3BpConfigsPrefix)
	if eventMetadata, exists := metadata[eventName]; exists {
		ret.Metadata = eventMetadata
	} else {
		ret.Metadata = map[string]bpdb.EventMetadataRow{}
	}
	writeStructToResponse(w, ret)
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
	err = validateEventMetadataUpdate(req.MetadataType, req.MetadataValue)
	if err != nil {
		core.NewServerWebError(err).ReportError(w, "Update event metadata validation error")
		return
	}

	defer s.goCache.Delete(allMetadataCache)
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
			Warning("'to_version' must be non-negative integer")
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

func (s *server) getMaintenanceMode(c web.C, w http.ResponseWriter, r *http.Request) {
	eventName, present := c.URLParams["schema"]
	var mm bpdb.MaintenanceMode
	if present {
		mm = s.bpdbBackend.GetSchemaMaintenanceMode(eventName)
		logger.WithField("schema", eventName).WithField("is_maintenance", mm.IsInMaintenanceMode).Info("Serving get schema maintenance mode request")
	} else {
		mm = s.bpdbBackend.GetMaintenanceMode()
		logger.WithField("is_maintenance", mm.IsInMaintenanceMode).Info("Serving get maintenance mode request")
	}
	js, err := json.Marshal(map[string]interface{}{"is_maintenance": mm.IsInMaintenanceMode, "user": mm.User})
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

func (s *server) setMaintenanceMode(c web.C, w http.ResponseWriter, r *http.Request) {
	var mm maintenanceMode
	err := decodeBody(r.Body, &mm)
	if err != nil {
		core.NewServerWebError(err).ReportError(w, "error parsing set maintenance mode request")
		return
	}
	user := c.Env["username"].(string)

	eventName, present := c.URLParams["schema"]
	if present {
		err = s.bpdbBackend.SetSchemaMaintenanceMode(eventName, mm.IsMaintenance, user, mm.Reason)
		if err != nil {
			core.NewServerWebError(err).ReportError(w, "error setting schema maintenance mode")
			return
		}
		logger.WithField("schema", eventName).WithField("is_maintenance", mm.IsMaintenance).WithField("reason", mm.Reason).Info("Schema maintenance mode set")
	} else {
		err = s.bpdbBackend.SetMaintenanceMode(mm.IsMaintenance, user, mm.Reason)
		if err != nil {
			core.NewServerWebError(err).ReportError(w, "error setting maintenance mode")
			return
		}
		logger.WithField("is_maintenance", mm.IsMaintenance).WithField("reason", mm.Reason).Info("Maintenance mode set")
	}
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
		return
	}
	publishToS3(s.s3Uploader, schemas, s.s3BpConfigsBucketName, kinesisConfigS3Key, s.s3BpConfigsPrefix)
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

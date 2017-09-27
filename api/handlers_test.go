package api

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zenazn/goji/web"

	"github.com/twitchscience/blueprint/bpdb"
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/blueprint/test"
	scoop "github.com/twitchscience/scoop_protocol/scoop_protocol"
)

func TestMigrationInvalidFrom(t *testing.T) {
	s3Uploader := NewMockS3Uploader()
	configFile := createJSONFile(t, "TestMigrationInvalidFrom")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, nil, configFile.Name(), nil, "", false, s3Uploader).(*server)
	handler := web.HandlerFunc(s.migration)
	recorder := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/migration/testerino?from_version=-4&to_version=4", nil)
	handler.ServeHTTP(recorder, req)
	if status := recorder.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}
	assertNotPublishedToS3(t, "TestMigrationInvalidFrom", s3Uploader)
}

func TestMigrationNegativeTo(t *testing.T) {
	s3Uploader := NewMockS3Uploader()
	configFile := createJSONFile(t, "TestMigrationNegativeTo")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, nil, configFile.Name(), nil, "", false, s3Uploader).(*server)
	handler := web.HandlerFunc(s.migration)
	recorder := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/migration/testerino?to_version=-4", nil)
	handler.ServeHTTP(recorder, req)
	if status := recorder.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}
	assertNotPublishedToS3(t, "TestMigrationNegativeTo", s3Uploader)
}

func TestBirthAdded(t *testing.T) {
	bpdbBackend := test.NewMockBpdb(map[string]bpdb.MaintenanceMode{}, []*bpdb.ActiveUser{}, []*bpdb.DailyChange{})
	schemaBackend := test.NewMockBpSchemaBackend()
	s3Uploader := NewMockS3Uploader()
	eventMetadataBackend := test.NewMockBpEventMetadataBackend(map[string]map[string]bpdb.EventMetadataRow{"event": {}})

	configFile := createJSONFile(t, "testCache")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", bpdbBackend, schemaBackend, nil, eventMetadataBackend, configFile.Name(), nil, "", false, s3Uploader).(*server)
	c := web.C{Env: map[interface{}]interface{}{"username": ""}}

	createSchema(t, s, c, schemaBackend)
	meta, err := eventMetadataBackend.AllEventMetadata()
	assert.Nil(t, err, "event metadata")
	birthTime, err := time.Parse("2006-01-02T15:04:05-0700", meta.Metadata["event"][string(scoop.BIRTH)].MetadataValue)
	assert.Nil(t, err, "parsing birth time")
	now := time.Now().UTC()
	if !(birthTime.Before(now) && birthTime.After(now.Add(-time.Minute))) {
		t.Errorf("Birth time of new event not in the created minute, birth time is: %v", birthTime)
	}
}

func TestAllSchemasCache(t *testing.T) {
	bpdbBackend := test.NewMockBpdb(map[string]bpdb.MaintenanceMode{}, []*bpdb.ActiveUser{}, []*bpdb.DailyChange{})
	schemaBackend := test.NewMockBpSchemaBackend()
	s3Uploader := NewMockS3Uploader()
	eventMetadataBackend := test.NewMockBpEventMetadataBackend(map[string]map[string]bpdb.EventMetadataRow{"event": {}})

	configFile := createJSONFile(t, "testCache")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", bpdbBackend, schemaBackend, nil, eventMetadataBackend, configFile.Name(), nil, "", false, s3Uploader).(*server)
	s.s3BpConfigsBucketName = "test-bucket"
	if s.cacheTimeout != time.Minute {
		t.Fatalf("cache timeout is %v, expected 1 minute", s.cacheTimeout)
	}
	c := web.C{Env: map[interface{}]interface{}{"username": ""}}

	printTotalAllSchemasCalls(t, schemaBackend)
	repeatAllSchema(t, s, schemaBackend)
	assertPublishedToS3(t, "repeatAllSchema", s3Uploader)
	createSchema(t, s, c, schemaBackend)
	assertPublishedToS3(t, "createSchema", s3Uploader)
	repeatAllSchema(t, s, schemaBackend)
	assertNotPublishedToS3(t, "repeatAllSchema", s3Uploader)
	createSchemaBlacklisted(t, s, c, schemaBackend)
	assertNotPublishedToS3(t, "createSchemaBlacklisted", s3Uploader)
	repeatAllSchema(t, s, schemaBackend)
	assertNotPublishedToS3(t, "repeatAllSchema", s3Uploader)
	updateSchema(t, s, c, schemaBackend)
	assertPublishedToS3(t, "updateSchema", s3Uploader)
	repeatAllSchema(t, s, schemaBackend)
	assertNotPublishedToS3(t, "repeatAllSchema", s3Uploader)

	if schemaBackend.GetAllSchemasCalls() != 3 {
		t.Errorf("AllSchemas() called %v times, expected 3", schemaBackend.GetAllSchemasCalls())
	}
}

func repeatAllSchema(t *testing.T, s *server, backend *test.MockBpSchemaBackend) {
	getAllReq, _ := http.NewRequest("GET", "/schemas", strings.NewReader(""))
	for i := 0; i < 3; i++ {
		getAllRecorder := httptest.NewRecorder()
		s.allSchemas(getAllRecorder, getAllReq)
		if getCachedSchemaResult(s) == nil {
			t.Error("Failed to cache result")
		}
		assertRequestOK(t, "allSchemas", getAllRecorder, "")
		printTotalAllSchemasCalls(t, backend)
	}
}

func createSchema(t *testing.T, s *server, c web.C, backend *test.MockBpSchemaBackend) {
	cfg := scoop.Config{EventName: "event", Columns: nil, Version: 0}
	cfgBytes, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal("unable to marshal scoop config, bailing")
	}

	createReq, _ := http.NewRequest("PUT", "/schema", bytes.NewReader(cfgBytes))
	createRecorder := httptest.NewRecorder()
	s.createSchema(c, createRecorder, createReq)
	assertRequestOK(t, "createSchema", createRecorder, "")
	printTotalAllSchemasCalls(t, backend)
}

func createSchemaBlacklisted(t *testing.T, s *server, c web.C, backend *test.MockBpSchemaBackend) {
	cfg := scoop.Config{EventName: "dfp_bad", Columns: nil, Version: 0}
	cfgBytes, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal("unable to marshal scoop config, bailing")
	}

	createReq, _ := http.NewRequest("PUT", "/schema", bytes.NewReader(cfgBytes))
	createRecorder := httptest.NewRecorder()
	s.createSchema(c, createRecorder, createReq)
	if status := createRecorder.Code; status != http.StatusBadRequest {
		t.Errorf("blacklisted createSchema returned status code %v, want %v",
			status, http.StatusBadRequest)
	}
	printTotalAllSchemasCalls(t, backend)
}

func updateSchema(t *testing.T, s *server, c web.C, backend *test.MockBpSchemaBackend) {
	updateSchemaReq := core.ClientUpdateSchemaRequest{}
	updateSchemaReqBytes, err := json.Marshal(updateSchemaReq)
	if err != nil {
		t.Fatal("unable to marshal scoop config, bailing")
	}

	updateReq, _ := http.NewRequest("POST", "/schema/1", bytes.NewReader(updateSchemaReqBytes))
	updateRecorder := httptest.NewRecorder()
	s.updateSchema(c, updateRecorder, updateReq)
	assertRequestOK(t, "updateSchema", updateRecorder, "")
	printTotalAllSchemasCalls(t, backend)
}

func getCachedSchemaResult(s *server) []bpdb.AnnotatedSchema {
	cachedSchemas, found := s.goCache.Get(allSchemasCache)
	if found {
		return cachedSchemas.([]bpdb.AnnotatedSchema)
	}
	return nil
}

func printTotalAllSchemasCalls(t *testing.T, backend *test.MockBpSchemaBackend) {
	t.Logf("AllSchemas() calls seen: %v", backend.GetAllSchemasCalls())
}

func assertRequestOK(t *testing.T, testedName string, w *httptest.ResponseRecorder, expectedResponse string) {
	response := strings.TrimSpace(w.Body.String())
	if w.Code != http.StatusOK {
		t.Errorf("%v returned status code %v, want %v, message: %v", testedName, w.Code, http.StatusOK, response)
	}
	if expectedResponse != "" && response != expectedResponse {
		t.Errorf("%v returned response [%v] does not match expected response [%v]", testedName, response, expectedResponse)
	}
}

func assertRequest404(t *testing.T, testedName string, w *httptest.ResponseRecorder) {
	if w.Code != http.StatusNotFound {
		t.Errorf("%v returned status code %v, want %v", testedName, w.Code, http.StatusNotFound)
	}
	errorMsg := strings.TrimSpace(w.Body.String())
	if errorMsg != "Not Found" {
		t.Errorf("%v returned error message [%v] does not match expected error message [%v]", testedName, errorMsg, "Not Found")
	}
}

func assertRequest503(t *testing.T, testedName string, w *httptest.ResponseRecorder) {
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("%v returned status code %v, want %v", testedName, w.Code, http.StatusServiceUnavailable)
	}
}

func assertRequestBad(t *testing.T, testedName string, w *httptest.ResponseRecorder, expectedErrorMsg string) {
	if w.Code != http.StatusBadRequest {
		t.Errorf("%v returned status code %v, want %v", testedName, w.Code, http.StatusBadRequest)
	}
	errorMsg := strings.TrimSpace(w.Body.String())
	expectedErrorMsg = strings.TrimSpace(expectedErrorMsg)
	if expectedErrorMsg != "" && errorMsg != expectedErrorMsg {
		t.Errorf("%v returned error message [%v] does not match expected error message [%v]", testedName, errorMsg, expectedErrorMsg)
	}
}

func assertRequestInternalError(t *testing.T, testedName string, w *httptest.ResponseRecorder, expectedErrorMsg string) {
	if w.Code != http.StatusInternalServerError {
		t.Errorf("%v returned status code %v, want %v", testedName, w.Code, http.StatusInternalServerError)
	}
	errorMsg := strings.TrimSpace(w.Body.String())
	expectedErrorMsg = strings.TrimSpace(expectedErrorMsg)
	if expectedErrorMsg != "" && errorMsg != expectedErrorMsg {
		t.Errorf("%v returned error message [%v] does not match expected error message [%v]", testedName, errorMsg, expectedErrorMsg)
	}
}

func assertPublishedToS3(t *testing.T, testedName string, s3Uploader *MockS3UploaderAPI) {
	if !s3Uploader.UploadSucceeded() {
		t.Errorf("%v did not successfully upload to S3", testedName)
	}
	s3Uploader.ResetUploadSuccess()
}

func assertNotPublishedToS3(t *testing.T, testedName string, s3Uploader *MockS3UploaderAPI) {
	if s3Uploader.UploadSucceeded() {
		t.Errorf("%v uploaded to S3 but was not expected to", testedName)
	}
}

func getCachedAllEventMetadataResult(s *server) map[string](map[string]bpdb.EventMetadataRow) {
	cachedAllEventMetadata, found := s.goCache.Get(allMetadataCache)
	if found {
		return cachedAllEventMetadata.(map[string](map[string]bpdb.EventMetadataRow))
	}
	return nil
}

func getCachedEventMetadataResult(s *server, eventName string) *bpdb.EventMetadata {
	cachedEventMetadata, found := s.goCache.Get(allMetadataCache)
	if found {
		eventMetadata, exists := cachedEventMetadata.(map[string](map[string]bpdb.EventMetadataRow))[eventName]
		if exists {
			return &bpdb.EventMetadata{
				EventName: eventName,
				Metadata:  eventMetadata,
			}
		}
	}
	return nil
}

func printTotalEventMetadataCalls(t *testing.T, backend *test.MockBpEventMetadataBackend) {
	t.Logf("EventMetadata() calls seen: %v", backend.GetAllEventMetadataCalls())
}

func getEventMetadata(c web.C, t *testing.T, s *server, backend *test.MockBpEventMetadataBackend, eventName string) {
	getReq, _ := http.NewRequest("GET", "/metadata/1", strings.NewReader(""))
	for i := 0; i < 3; i++ {
		getRecorder := httptest.NewRecorder()
		s.eventMetadata(c, getRecorder, getReq)
		if getCachedEventMetadataResult(s, eventName) == nil {
			t.Error("Failed to cache result")
		}
		assertRequestOK(t, "getEventMetadata", getRecorder, "")
		printTotalEventMetadataCalls(t, backend)
	}
}

func updateEventMetadata(t *testing.T, s *server, c web.C, backend *test.MockBpEventMetadataBackend, eventName string) {
	updateEventMetadataReq := core.ClientUpdateEventMetadataRequest{
		EventName:     eventName,
		MetadataType:  "edge_type",
		MetadataValue: "internal",
	}
	updateEventMetadataReqBytes, err := json.Marshal(updateEventMetadataReq)
	if err != nil {
		t.Fatal("unable to marshal scoop config, bailing")
	}

	updateReq, _ := http.NewRequest("POST", "/metadata/1", bytes.NewReader(updateEventMetadataReqBytes))
	updateRecorder := httptest.NewRecorder()
	s.updateEventMetadata(c, updateRecorder, updateReq)
	assertRequestOK(t, "updateEventMetadata", updateRecorder, "")
	printTotalEventMetadataCalls(t, backend)
}

func repeatAllEventMetadata(t *testing.T, s *server, backend *test.MockBpEventMetadataBackend) {
	getAllReq, _ := http.NewRequest("GET", "/allmetadata", strings.NewReader(""))
	for i := 0; i < 3; i++ {
		getAllRecorder := httptest.NewRecorder()
		s.allEventMetadata(getAllRecorder, getAllReq)
		if len(getCachedAllEventMetadataResult(s)) == 0 {
			t.Error("Failed to cache result")
		}
		assertRequestOK(t, "allMetadata", getAllRecorder, "")
		printTotalEventMetadataCalls(t, backend)
	}
}

func TestAllEventMetadataCache(t *testing.T) {
	eventMetadataMap := make(map[string]map[string]bpdb.EventMetadataRow)
	eventMetadataMap["this-table-exists"] = map[string]bpdb.EventMetadataRow{
		"comment": {
			MetadataValue: "Test comment",
			UserName:      "legacy",
			Version:       2,
		},
	}
	schemaBackend := test.NewMockBpSchemaBackend()
	eventMetadataBackend := test.NewMockBpEventMetadataBackend(eventMetadataMap)
	s3Uploader := NewMockS3Uploader()
	configFile := createJSONFile(t, "TestAllEventMetadataCache")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)
	s := New("", nil, schemaBackend, nil, eventMetadataBackend, configFile.Name(), nil, "", false, s3Uploader).(*server)
	s.s3BpConfigsBucketName = "test-bucket"

	if s.cacheTimeout != time.Minute {
		t.Fatalf("cache timeout is %v, expected 1 minute", s.cacheTimeout)
	}
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"username": "", "event": "this-table-exists"},
	}

	printTotalEventMetadataCalls(t, eventMetadataBackend)
	repeatAllEventMetadata(t, s, eventMetadataBackend)
	assertPublishedToS3(t, "repeatAllEventMetadata", s3Uploader)
	getEventMetadata(c, t, s, eventMetadataBackend, "this-table-exists")
	assertNotPublishedToS3(t, "getEventMetadata", s3Uploader)
	updateEventMetadata(t, s, c, eventMetadataBackend, "this-table-exists")
	assertPublishedToS3(t, "updateEventMetadata", s3Uploader)
	getEventMetadata(c, t, s, eventMetadataBackend, "this-table-exists")
	assertNotPublishedToS3(t, "getEventMetadata", s3Uploader)
	repeatAllEventMetadata(t, s, eventMetadataBackend)
	assertNotPublishedToS3(t, "repeatAllEventMetadata", s3Uploader)
	repeatAllEventMetadata(t, s, eventMetadataBackend)
	assertNotPublishedToS3(t, "repeatAllEventMetadata", s3Uploader)
	updateEventMetadata(t, s, c, eventMetadataBackend, "this-table-exists")
	assertPublishedToS3(t, "updateEventMetadata", s3Uploader)
	repeatAllEventMetadata(t, s, eventMetadataBackend)
	assertNotPublishedToS3(t, "repeatAllEventMetadata", s3Uploader)
	getEventMetadata(c, t, s, eventMetadataBackend, "this-table-exists")
	assertNotPublishedToS3(t, "getEventMetadata", s3Uploader)
	updateEventMetadata(t, s, c, eventMetadataBackend, "this-table-exists")
	assertPublishedToS3(t, "updateEventMetadata", s3Uploader)
	getEventMetadata(c, t, s, eventMetadataBackend, "this-table-exists")
	assertNotPublishedToS3(t, "getEventMetadata", s3Uploader)

	if eventMetadataBackend.GetAllEventMetadataCalls() != 4 {
		t.Errorf("EventMetadata() called %v times, expected 4", eventMetadataBackend.GetAllEventMetadataCalls())
	}
}

// Tests trying to get metadata for an event with no schema
// Expected result is a 404 not found
func TestGetEventMetadataNotFound(t *testing.T) {
	schemaBackend := test.NewMockBpSchemaBackend()
	eventMetadataMap := make(map[string]map[string]bpdb.EventMetadataRow)
	eventMetadataBackend := test.NewMockBpEventMetadataBackend(eventMetadataMap)
	s3Uploader := NewMockS3Uploader()
	configFile := createJSONFile(t, "TestGetEventMetadataNotFound")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, schemaBackend, nil, eventMetadataBackend, configFile.Name(), nil, "", false, s3Uploader).(*server)
	recorder := httptest.NewRecorder()
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"username": "", "event": "this-table-does-not-exist"},
	}
	req, _ := http.NewRequest("GET", "/metadata/this-table-does-not-exist", nil)
	s.eventMetadata(c, recorder, req)

	assertRequest404(t, "TestGetEventMetadataNotFound", recorder)
	assertNotPublishedToS3(t, "TestGetEventMetadataNotFound", s3Uploader)
}

// Tests trying to get metadata for an event with a schema
// Expected result is a 200 OK response
func TestGetEventMetadata(t *testing.T) {
	schemaBackend := test.NewMockBpSchemaBackend()
	eventMetadataMap := make(map[string]map[string]bpdb.EventMetadataRow)
	eventMetadataMap["this-table-exists"] = map[string]bpdb.EventMetadataRow{
		"comment": bpdb.EventMetadataRow{
			MetadataValue: "Test comment",
			UserName:      "legacy",
			Version:       2,
		},
	}
	eventMetadataBackend := test.NewMockBpEventMetadataBackend(eventMetadataMap)
	s3Uploader := NewMockS3Uploader()
	configFile := createJSONFile(t, "TestGetEventMetadata")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, schemaBackend, nil, eventMetadataBackend, configFile.Name(), nil, "", false, s3Uploader).(*server)
	s.s3BpConfigsBucketName = "test-bucket"
	recorder := httptest.NewRecorder()
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"username": "", "event": "this-table-exists"},
	}
	req, _ := http.NewRequest("GET", "/metadata/this-table-exists", nil)

	s.eventMetadata(c, recorder, req)
	expectedBody := "{\"EventName\":\"this-table-exists\",\"Metadata\":{\"comment\":{\"MetadataValue\":" +
		"\"Test comment\",\"TS\":\"0001-01-01T00:00:00Z\",\"UserName\":\"legacy\",\"Version\":2}}}"
	assertRequestOK(t, "TestGetEventMetadata", recorder, expectedBody)
	assertPublishedToS3(t, "TestGetEventMetadata", s3Uploader)
}

// Tests trying to update metadata for an event with no schema
// Expected result is a 400 bad request
func TestUpdateEventMetadataNoSchema(t *testing.T) {
	eventMetadataMap := make(map[string]map[string]bpdb.EventMetadataRow)
	backend := test.NewMockBpEventMetadataBackend(eventMetadataMap)
	s3Uploader := NewMockS3Uploader()
	configFile := createJSONFile(t, "TestUpdateEventMetadataNoSchema")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, backend, configFile.Name(), nil, "", false, s3Uploader).(*server)
	s.s3BpConfigsBucketName = "test-bucket"
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"username": "", "event": "this-table-does-not-exist"},
	}

	cfg := core.ClientUpdateEventMetadataRequest{EventName: "this-table-does-not-exist", MetadataType: "edge_type", MetadataValue: "internal"}
	cfgBytes, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal("unable to marshal scoop config, bailing")
	}

	recorder := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", "/metadata/this-table-does-not-exist", bytes.NewReader(cfgBytes))

	s.updateEventMetadata(c, recorder, req)
	assertRequestBad(t, "TestUpdateEventMetadataNoSchema", recorder, "Error updating event metadata: schema does not exist")
	assertNotPublishedToS3(t, "TestUpdateEventMetadataNoSchema", s3Uploader)
}

// Tests trying to update metadata for an invalid metadata type
// Expected result is a 500 internal error
func TestUpdateEventMetadataInvalidMetadataType(t *testing.T) {
	eventMetadataMap := make(map[string]map[string]bpdb.EventMetadataRow)
	eventMetadataMap["test-event"] = map[string]bpdb.EventMetadataRow{
		"test-event": {},
	}

	backend := test.NewMockBpEventMetadataBackend(eventMetadataMap)
	s3Uploader := NewMockS3Uploader()
	configFile := createJSONFile(t, "TestUpdateEventMetadataInvalidMetadataType")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, backend, configFile.Name(), nil, "", false, s3Uploader).(*server)
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"username": "", "event": "test-event"},
	}

	cfg := core.ClientUpdateEventMetadataRequest{EventName: "test-event", MetadataType: "invalid_type", MetadataValue: "Test"}
	cfgBytes, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal("unable to marshal scoop config, bailing")
	}

	recorder := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", "/metadata/test-event", bytes.NewReader(cfgBytes))

	s.updateEventMetadata(c, recorder, req)
	assertRequestInternalError(t, "TestUpdateEventMetadataInvalidMetadataType", recorder, "Internal error: Update event metadata validation error")
	assertNotPublishedToS3(t, "TestUpdateEventMetadataInvalidMetadataType", s3Uploader)
}

// Tests trying to update metadata for an event with a schema
// Expected result is a 200 OK response
func TestUpdateEventMetadata(t *testing.T) {
	eventMetadataMap := make(map[string]map[string]bpdb.EventMetadataRow)
	eventMetadataMap["this-table-exists"] = map[string]bpdb.EventMetadataRow{}
	backend := test.NewMockBpEventMetadataBackend(eventMetadataMap)
	s3Uploader := NewMockS3Uploader()
	configFile := createJSONFile(t, "TestUpdateEventMetadata")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, backend, configFile.Name(), nil, "", false, s3Uploader).(*server)
	s.s3BpConfigsBucketName = "test-bucket"
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"username": "", "event": "this-table-exists"},
	}

	cfg := core.ClientUpdateEventMetadataRequest{EventName: "this-table-exists", MetadataType: "edge_type", MetadataValue: "internal"}
	cfgBytes, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal("unable to marshal scoop config, bailing")
	}

	createRecorder := httptest.NewRecorder()
	createReq, _ := http.NewRequest("PUT", "/metadata/this-table-exists", bytes.NewReader(cfgBytes))

	s.updateEventMetadata(c, createRecorder, createReq)
	assertRequestOK(t, "TestUpdateEventMetadata", createRecorder, "")
	assertPublishedToS3(t, "TestUpdateEventMetadata", s3Uploader)
}

func TestDecodeBody(t *testing.T) {
	r := strings.NewReader(`{
		"StreamName": "spade-downstream-prod-test",
		"StreamRole": "arn:aws:iam::123:role/spade-downstream-prod-test",
		"StreamType": "firehose",
		"EventNameTargetField": "name",
		"Compress": false,
		"Events": {
			"minute-watched": {
				"Fields": [
					"time"
				]
			}
		}
	}`)
	var config scoop.KinesisWriterConfig
	err := decodeBody(ioutil.NopCloser(r), &config)
	require.Nil(t, err)
	assert.Equal(t, "spade-downstream-prod-test", config.StreamName)
	assert.Equal(t, "arn:aws:iam::123:role/spade-downstream-prod-test", config.StreamRole)
	assert.Equal(t, "firehose", config.StreamType)
	assert.Equal(t, "name", config.EventNameTargetField)
	assert.Equal(t, false, config.Compress)
	assert.Equal(t, []string{"time"}, config.Events["minute-watched"].Fields)
}

func TestSchemaNegativeVersion(t *testing.T) {
	s3Uploader := NewMockS3Uploader()
	configFile := createJSONFile(t, "TestSchemaNegativeVersion")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, nil, configFile.Name(), nil, "", false, s3Uploader).(*server)
	handler := web.HandlerFunc(s.schema)
	recorder := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/schema/empty-table?version=-4", nil)
	handler.ServeHTTP(recorder, req)
	if status := recorder.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}
	assertNotPublishedToS3(t, "TestSchemaNegativeVersion", s3Uploader)
}

func TestSchemaMaintenanceGet(t *testing.T) {
	bpdbBackend := test.NewMockBpdb(map[string]bpdb.MaintenanceMode{
		"in-maintenance": bpdb.MaintenanceMode{IsInMaintenanceMode: true, User: "bob"},
	}, []*bpdb.ActiveUser{}, []*bpdb.DailyChange{})
	schemaBackend := test.NewMockBpSchemaBackend()
	s3Uploader := NewMockS3Uploader()
	configFile := createJSONFile(t, "testSchemaMaintenanceGet")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", bpdbBackend, schemaBackend, nil, nil, configFile.Name(), nil, "", false, s3Uploader).(*server)

	recorder := httptest.NewRecorder()
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"schema": "in-maintenance"},
	}
	req, _ := http.NewRequest("GET", "/maintenance/in-maintenance", nil)
	s.getMaintenanceMode(c, recorder, req)

	assertRequestOK(t, "TestMaintenanceGet", recorder, `{"is_maintenance":true,"user":"bob"}`)
	assertNotPublishedToS3(t, "TestMaintenanceGet", s3Uploader)
}

func TestSchemaMaintenanceSet(t *testing.T) {
	bpdbBackend := test.NewMockBpdb(map[string]bpdb.MaintenanceMode{
		"starts-in-maintenance": bpdb.MaintenanceMode{IsInMaintenanceMode: true, User: "bob"},
	}, []*bpdb.ActiveUser{}, []*bpdb.DailyChange{})
	schemaBackend := test.NewMockBpSchemaBackend()
	s3Uploader := NewMockS3Uploader()
	configFile := createJSONFile(t, "testSchemaMaintenanceSet")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", bpdbBackend, schemaBackend, nil, nil, configFile.Name(), nil, "", false, s3Uploader).(*server)

	recorder := httptest.NewRecorder()
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"schema": "starts-in-maintenance"},
	}

	mm := maintenanceMode{IsMaintenance: false, Reason: "for testing"}
	cfgBytes, err := json.Marshal(mm)
	if err != nil {
		t.Fatal("unable to marshal maintenance mode request body, bailing")
	}
	req, _ := http.NewRequest("POST", "/maintenance/starts-in-maintenance", bytes.NewReader(cfgBytes))
	s.setMaintenanceMode(c, recorder, req)

	assertRequestOK(t, "testSchemaMaintenanceSet", recorder, "")
	assertNotPublishedToS3(t, "testSchemaMaintenanceSet", s3Uploader)
}

func TestUpdateDuringSchemaMaintenance(t *testing.T) {
	bpdbBackend := test.NewMockBpdb(map[string]bpdb.MaintenanceMode{
		"starts-in-maintenance": bpdb.MaintenanceMode{IsInMaintenanceMode: true, User: "bob"},
	}, []*bpdb.ActiveUser{}, []*bpdb.DailyChange{})
	schemaBackend := test.NewMockBpSchemaBackend()
	s3Uploader := NewMockS3Uploader()
	configFile := createJSONFile(t, "TestUpdateDuringSchemaMaintenance")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", bpdbBackend, schemaBackend, nil, nil, configFile.Name(), nil, "", false, s3Uploader).(*server)

	recorder := httptest.NewRecorder()
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"id": "starts-in-maintenance"},
	}

	req, _ := http.NewRequest("POST", "/schema/starts-in-maintenance", nil)
	s.updateSchema(c, recorder, req)
	assertRequest503(t, "TestUpdateDuringSchemaMaintenance", recorder)
	assertNotPublishedToS3(t, "TestUpdateDuringSchemaMaintenance", s3Uploader)
}

func TestUpdateDuringGlobalMaintenance(t *testing.T) {
	bpdbBackend := test.NewMockBpdb(map[string]bpdb.MaintenanceMode{}, []*bpdb.ActiveUser{}, []*bpdb.DailyChange{})
	err := bpdbBackend.SetMaintenanceMode(true, "test", "because I'm an automated test.")
	assert.NoError(t, err)
	schemaBackend := test.NewMockBpSchemaBackend()
	s3Uploader := NewMockS3Uploader()
	configFile := createJSONFile(t, "TestUpdateDuringGlobalMaintenance")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", bpdbBackend, schemaBackend, nil, nil, configFile.Name(), nil, "", false, s3Uploader).(*server)
	ts := httptest.NewServer(s.maintenanceHandler(getTestHandler()))
	defer ts.Close()
	var u bytes.Buffer
	u.WriteString(string(ts.URL))
	u.WriteString("/schema/whatever")
	res, err := http.Get(u.String())
	assert.NoError(t, err)

	if res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("TestUpdateDuringGlobalMaintenance returned status code %v, want %v", res.StatusCode, http.StatusServiceUnavailable)
	}
	assertNotPublishedToS3(t, "TestUpdateDuringGlobalMaintenance", s3Uploader)
}

// Tests getting the ValidTransform types
// Expected result is a 200 OK response
func TestGetValidTransformTypes(t *testing.T) {
	s3Uploader := NewMockS3Uploader()
	configFile := createJSONFile(t, "TestGetValidTransformTypes")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, nil, configFile.Name(), nil, "", false, s3Uploader).(*server)
	recorder := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/types", nil)

	s.types(recorder, req)
	expectedBody := "{\"result\":[\"bigint\",\"bool\",\"float\",\"int\",\"ipAsn\",\"ipAsnInteger\",\"ipCity\",\"ipCountry\"," +
		"\"ipRegion\",\"varchar\",\"f@timestamp@unix\",\"f@timestamp@unix-utc\",\"userIDWithMapping\"]}"
	assertRequestOK(t, "TestGetValidTransformTypes", recorder, expectedBody)
	assertNotPublishedToS3(t, "TestGetValidTransformTypes", s3Uploader)
}

// Tests getting the DailyChanges and ActiveUsers
// Expected result is a 200 OK response
func TestGetStats(t *testing.T) {
	activeUsers := []*bpdb.ActiveUser{
		&bpdb.ActiveUser{
			UserName: "legacy",
			Changes:  2,
		},
		&bpdb.ActiveUser{
			UserName: "unknown",
			Changes:  3,
		},
	}
	dailyChanges := []*bpdb.DailyChange{
		&bpdb.DailyChange{
			Day:     "2017-07-18T00:00:00Z",
			Changes: 6,
			Users:   3,
		},
	}
	bpdbBackend := test.NewMockBpdb(map[string]bpdb.MaintenanceMode{}, activeUsers, dailyChanges)
	s3Uploader := NewMockS3Uploader()
	configFile := createJSONFile(t, "TestGetStats")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", bpdbBackend, nil, nil, nil, configFile.Name(), nil, "", false, s3Uploader).(*server)
	recorder := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/stats", nil)

	s.stats(recorder, req)
	expectedBody := "{\"DailyChanges\":[{\"Day\":\"2017-07-18T00:00:00Z\",\"Changes\":6,\"Users\":3}]," +
		"\"ActiveUsers\":[{\"UserName\":\"legacy\",\"Changes\":2},{\"UserName\":\"unknown\",\"Changes\":3}]}"
	assertRequestOK(t, "TestGetStats", recorder, expectedBody)
	assertNotPublishedToS3(t, "TestGetStats", s3Uploader)
}

// GetTestHandler returns a http.HandlerFunc for testing http middleware
func getTestHandler() http.HandlerFunc {
	fn := func(rw http.ResponseWriter, req *http.Request) {
		panic("test entered test handler, this should not happen")
	}
	return http.HandlerFunc(fn)
}

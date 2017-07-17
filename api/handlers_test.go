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

func TestMigrationNegativeTo(t *testing.T) {
	configFile := createJSONFile(t, "testMigration")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, nil, configFile.Name(), nil, "", false, NewMockS3Uploader()).(*server)
	handler := web.HandlerFunc(s.migration)
	recorder := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/migration/testerino?to_version=-4", nil)
	handler.ServeHTTP(recorder, req)
	if status := recorder.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}
}

func TestAllSchemasCache(t *testing.T) {
	bpdbBackend := test.NewMockBpdb(map[string]bpdb.MaintenanceMode{})
	schemaBackend := test.NewMockBpSchemaBackend()

	configFile := createJSONFile(t, "testCache")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", bpdbBackend, schemaBackend, nil, nil, configFile.Name(), nil, "", false, NewMockS3Uploader()).(*server)
	if s.cacheTimeout != time.Minute {
		t.Fatalf("cache timeout is %v, expected 1 minute", s.cacheTimeout)
	}
	c := web.C{Env: map[interface{}]interface{}{"username": ""}}

	printTotalAllSchemasCalls(t, schemaBackend)
	repeatAllSchema(t, s, schemaBackend)
	createSchema(t, s, c, schemaBackend)
	repeatAllSchema(t, s, schemaBackend)
	createSchemaBlacklisted(t, s, c, schemaBackend)
	repeatAllSchema(t, s, schemaBackend)
	updateSchema(t, s, c, schemaBackend)
	repeatAllSchema(t, s, schemaBackend)

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
	if getCachedSchemaResult(s) != nil {
		t.Error("Failed to invalidate cache")
	}
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
	if getCachedSchemaResult(s) != nil {
		t.Error("Failed to invalidate cache")
	}
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
	if w.Code != http.StatusOK {
		t.Errorf("%v returned status code %v, want %v", testedName, w.Code, http.StatusOK)
	}
	response := strings.TrimSpace(w.Body.String())
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
	if len(getCachedAllEventMetadataResult(s)) > 0 {
		t.Error("Failed to invalidate cache")
	}
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
	eventMetadataMap := make(map[string]bpdb.EventMetadata)
	eventMetadataMap["this-table-exists"] = bpdb.EventMetadata{
		Metadata: map[string]bpdb.EventMetadataRow{
			"comment": bpdb.EventMetadataRow{
				MetadataValue: "Test comment",
				UserName:      "legacy",
				Version:       2,
			},
		},
	}
	schemaBackend := test.NewMockBpSchemaBackend()
	eventMetadataBackend := test.NewMockBpEventMetadataBackend(eventMetadataMap)
	configFile := createJSONFile(t, "TestAllEventMetadataCache")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)
	// CHANGE THIS BACK TO MOCK UPLOADER AFTER
	s := New("", nil, schemaBackend, nil, eventMetadataBackend, configFile.Name(), nil, "", false, NewS3Uploader()).(*server)

	if s.cacheTimeout != time.Minute {
		t.Fatalf("cache timeout is %v, expected 1 minute", s.cacheTimeout)
	}
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"username": "", "event": "this-table-exists"},
	}

	printTotalEventMetadataCalls(t, eventMetadataBackend)
	repeatAllEventMetadata(t, s, eventMetadataBackend)
	getEventMetadata(c, t, s, eventMetadataBackend, "this-table-exists")
	updateEventMetadata(t, s, c, eventMetadataBackend, "this-table-exists")
	getEventMetadata(c, t, s, eventMetadataBackend, "this-table-exists")
	repeatAllEventMetadata(t, s, eventMetadataBackend)
	repeatAllEventMetadata(t, s, eventMetadataBackend)
	updateEventMetadata(t, s, c, eventMetadataBackend, "this-table-exists")
	repeatAllEventMetadata(t, s, eventMetadataBackend)
	getEventMetadata(c, t, s, eventMetadataBackend, "this-table-exists")
	updateEventMetadata(t, s, c, eventMetadataBackend, "this-table-exists")
	getEventMetadata(c, t, s, eventMetadataBackend, "this-table-exists")

	if eventMetadataBackend.GetAllEventMetadataCalls() != 4 {
		t.Errorf("EventMetadata() called %v times, expected 4", eventMetadataBackend.GetAllEventMetadataCalls())
	}
}

// Tests trying to get metadata for an event with no schema
// Expected result is a 404 not found
func TestGetEventMetadataNotFound(t *testing.T) {
	schemaBackend := test.NewMockBpSchemaBackend()
	eventMetadataMap := make(map[string]bpdb.EventMetadata)
	eventMetadataBackend := test.NewMockBpEventMetadataBackend(eventMetadataMap)
	configFile := createJSONFile(t, "TestGetEventMetadataNotFound")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, schemaBackend, nil, eventMetadataBackend, configFile.Name(), nil, "", false, NewMockS3Uploader()).(*server)
	recorder := httptest.NewRecorder()
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"username": "", "event": "this-table-does-not-exist"},
	}
	req, _ := http.NewRequest("GET", "/metadata/this-table-does-not-exist", nil)
	s.eventMetadata(c, recorder, req)

	assertRequest404(t, "TestGetEventMetadataNotFound", recorder)
}

// Tests trying to get metadata for an event with a schema
// Expected result is a 200 OK response
func TestGetEventMetadata(t *testing.T) {
	schemaBackend := test.NewMockBpSchemaBackend()
	eventMetadataMap := make(map[string]bpdb.EventMetadata)
	eventMetadataMap["this-table-exists"] = bpdb.EventMetadata{
		EventName: "event",
		Metadata: map[string]bpdb.EventMetadataRow{
			"comment": bpdb.EventMetadataRow{
				MetadataValue: "Test comment",
				UserName:      "legacy",
				Version:       2,
			},
		},
	}
	eventMetadataBackend := test.NewMockBpEventMetadataBackend(eventMetadataMap)
	configFile := createJSONFile(t, "TestGetEventMetadata")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, schemaBackend, nil, eventMetadataBackend, configFile.Name(), nil, "", false, NewMockS3Uploader()).(*server)
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
}

// Tests trying to update metadata for an event with no schema
// Expected result is a 400 bad request
func TestUpdateEventMetadataNoSchema(t *testing.T) {
	eventMetadataMap := make(map[string]bpdb.EventMetadata)
	backend := test.NewMockBpEventMetadataBackend(eventMetadataMap)
	configFile := createJSONFile(t, "TestUpdateEventMetadataNoSchema")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, backend, configFile.Name(), nil, "", false, NewMockS3Uploader()).(*server)
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
}

// Tests trying to update metadata for an invalid metadata type
// Expected result is a 500 internal error
func TestUpdateEventMetadataInvalidMetadataType(t *testing.T) {
	eventMetadataMap := make(map[string]bpdb.EventMetadata)
	eventMetadataMap["test-event"] = bpdb.EventMetadata{
		EventName: "test-event",
	}
	backend := test.NewMockBpEventMetadataBackend(eventMetadataMap)
	configFile := createJSONFile(t, "TestUpdateEventMetadataInvalidMetadataType")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, backend, configFile.Name(), nil, "", false, NewMockS3Uploader()).(*server)
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
}

// Tests trying to update metadata for an event with a schema
// Expected result is a 200 OK response
func TestUpdateEventMetadata(t *testing.T) {
	eventMetadataMap := make(map[string]bpdb.EventMetadata)
	eventMetadataMap["this-table-exists"] = bpdb.EventMetadata{}
	backend := test.NewMockBpEventMetadataBackend(eventMetadataMap)
	configFile := createJSONFile(t, "TestUpdateEventMetadata")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, backend, configFile.Name(), nil, "", false, NewMockS3Uploader()).(*server)
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

func TestSchemaMaintenanceGet(t *testing.T) {
	bpdbBackend := test.NewMockBpdb(map[string]bpdb.MaintenanceMode{
		"in-maintenance": bpdb.MaintenanceMode{IsInMaintenanceMode: true, User: "bob"},
	})
	schemaBackend := test.NewMockBpSchemaBackend()
	configFile := createJSONFile(t, "testSchemaMaintenanceGet")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", bpdbBackend, schemaBackend, nil, nil, configFile.Name(), nil, "", false, NewMockS3Uploader()).(*server)

	recorder := httptest.NewRecorder()
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"schema": "in-maintenance"},
	}
	req, _ := http.NewRequest("GET", "/maintenance/in-maintenance", nil)
	s.getMaintenanceMode(c, recorder, req)

	assertRequestOK(t, "TestMaintenanceGet", recorder, `{"is_maintenance":true,"user":"bob"}`)
}

func TestSchemaMaintenanceSet(t *testing.T) {
	bpdbBackend := test.NewMockBpdb(map[string]bpdb.MaintenanceMode{
		"starts-in-maintenance": bpdb.MaintenanceMode{IsInMaintenanceMode: true, User: "bob"},
	})
	schemaBackend := test.NewMockBpSchemaBackend()
	configFile := createJSONFile(t, "testSchemaMaintenanceSet")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", bpdbBackend, schemaBackend, nil, nil, configFile.Name(), nil, "", false, NewMockS3Uploader()).(*server)

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
}

func TestUpdateDuringSchemaMaintenance(t *testing.T) {
	bpdbBackend := test.NewMockBpdb(map[string]bpdb.MaintenanceMode{
		"starts-in-maintenance": bpdb.MaintenanceMode{IsInMaintenanceMode: true, User: "bob"},
	})
	schemaBackend := test.NewMockBpSchemaBackend()
	configFile := createJSONFile(t, "TestUpdateDuringSchemaMaintenance")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", bpdbBackend, schemaBackend, nil, nil, configFile.Name(), nil, "", false, NewMockS3Uploader()).(*server)

	recorder := httptest.NewRecorder()
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"id": "starts-in-maintenance"},
	}

	req, _ := http.NewRequest("POST", "/schema/starts-in-maintenance", nil)
	s.updateSchema(c, recorder, req)
	assertRequest503(t, "TestUpdateDuringSchemaMaintenance", recorder)
}

func TestUpdateDuringGlobalMaintenance(t *testing.T) {
	bpdbBackend := test.NewMockBpdb(map[string]bpdb.MaintenanceMode{})
	err := bpdbBackend.SetMaintenanceMode(true, "test", "because I'm an automated test.")
	assert.NoError(t, err)
	schemaBackend := test.NewMockBpSchemaBackend()
	configFile := createJSONFile(t, "TestUpdateDuringGlobalMaintenance")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", bpdbBackend, schemaBackend, nil, nil, configFile.Name(), nil, "", false, NewMockS3Uploader()).(*server)
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
}

// GetTestHandler returns a http.HandlerFunc for testing http middleware
func getTestHandler() http.HandlerFunc {
	fn := func(rw http.ResponseWriter, req *http.Request) {
		panic("test entered test handler, this should not happen")
	}
	return http.HandlerFunc(fn)
}

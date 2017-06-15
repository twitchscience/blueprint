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

	s := New("", nil, nil, nil, nil, nil, configFile.Name(), nil, "", false).(*server)
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
	backend := test.NewMockBpSchemaBackend()

	configFile := createJSONFile(t, "testCache")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, backend, nil, nil, nil, configFile.Name(), nil, "", false).(*server)
	if s.cacheTimeout != time.Minute {
		t.Fatalf("cache timeout is %v, expected 1 minute", s.cacheTimeout)
	}
	c := web.C{Env: map[interface{}]interface{}{"username": ""}}

	printTotalAllSchemasCalls(t, backend)
	repeatAllSchema(t, s, backend)
	createSchema(t, s, c, backend)
	repeatAllSchema(t, s, backend)
	createSchemaBlacklisted(t, s, c, backend)
	repeatAllSchema(t, s, backend)
	updateSchema(t, s, c, backend)
	repeatAllSchema(t, s, backend)

	if backend.GetAllSchemasCalls() != 3 {
		t.Errorf("AllSchemas() called %v times, expected 3", backend.GetAllSchemasCalls())
	}
	if getCachedVersion(s) != 3 {
		t.Errorf("Cached version is %v, expected 3", getCachedVersion(s))
	}
}

func repeatAllSchema(t *testing.T, s *server, backend *test.MockBpSchemaBackend) {
	getAllReq, _ := http.NewRequest("GET", "/schemas", strings.NewReader(""))
	for i := 0; i < 3; i++ {
		getAllRecorder := httptest.NewRecorder()
		s.allSchemas(getAllRecorder, getAllReq)
		if getCachedResult(s) == nil {
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
	if getCachedResult(s) != nil {
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
	if getCachedResult(s) != nil {
		t.Error("Failed to invalidate cache")
	}
	printTotalAllSchemasCalls(t, backend)
}

func getCachedResult(s *server) *schemaResult {
	c := make(chan *schemaResult)
	s.cacheSynchronizer <- func() { c <- s.cachedResult }
	return <-c
}

func getCachedVersion(s *server) int {
	c := make(chan int)
	s.cacheSynchronizer <- func() { c <- s.cachedVersion }
	return <-c
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

// Tests trying to get a comment for an event with no schema
// Expected result is a 500 internal error
func TestGetEventCommentNotFound(t *testing.T) {
	eventCommentMap := make(map[string]bpdb.EventComment)
	backend := test.NewMockBpEventCommentBackend(eventCommentMap)
	configFile := createJSONFile(t, "TestGetEventCommentNotFound")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, backend, nil, configFile.Name(), nil, "", false).(*server)
	recorder := httptest.NewRecorder()
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"username": "", "event": "this-table-does-not-exist"},
	}
	req, _ := http.NewRequest("GET", "/comment/this-table-does-not-exist", nil)
	s.eventComment(c, recorder, req)

	expectedErrorMsg := "no comment found for event this-table-does-not-exist"
	assertRequestInternalError(t, "TestGetEventCommentNotFound", recorder, expectedErrorMsg)
}

// Tests trying to get a comment for an event with a schema
// Expected result is a 200 OK response
func TestGetEventComment(t *testing.T) {
	eventCommentMap := make(map[string]bpdb.EventComment)
	eventCommentMap["this-table-exists-and-has-a-comment"] = bpdb.EventComment{
		EventName: "event",
		Comment:   "Test Comment",
		UserName:  "unknown",
		Version:   1,
	}
	backend := test.NewMockBpEventCommentBackend(eventCommentMap)
	configFile := createJSONFile(t, "TestGetEventComment")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, backend, nil, configFile.Name(), nil, "", false).(*server)
	recorder := httptest.NewRecorder()
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"username": "", "event": "this-table-exists-and-has-a-comment"},
	}
	req, _ := http.NewRequest("GET", "/comment/this-table-exists-and-has-a-comment", nil)

	s.eventComment(c, recorder, req)
	expectedBody := "[{\"EventName\":\"event\",\"Comment\":\"Test Comment\",\"TS\":\"0001-01-01T00:00:00Z\",\"UserName\":\"unknown\",\"Version\":1}]"
	assertRequestOK(t, "TestGetEventComment", recorder, expectedBody)
}

// Tests trying to update a comment for an event with no schema
// Expected result is a 400 bad request
func TestUpdateEventCommentNoSchema(t *testing.T) {
	eventCommentMap := make(map[string]bpdb.EventComment)
	eventCommentMap["this-table-does-not-exist"] = bpdb.EventComment{}
	backend := test.NewMockBpEventCommentBackend(eventCommentMap)
	configFile := createJSONFile(t, "TestUpdateEventCommentNoSchema")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, backend, nil, configFile.Name(), nil, "", false).(*server)
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"username": "", "event": "this-table-does-not-exist"},
	}

	cfg := scoop.EventComment{EventName: "this-table-does-not-exist", EventComment: "Test Comment", UserName: ""}
	cfgBytes, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal("unable to marshal scoop config, bailing")
	}

	recorder := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", "/comment/this-table-does-not-exist", bytes.NewReader(cfgBytes))

	s.updateEventComment(c, recorder, req)
	assertRequestBad(t, "TestUpdateEventCommentNoSchema", recorder, "Error updating event comment: schema does not exist")
}

// Tests trying to update a comment for an event with a schema
// Expected result is a 200 OK response
func TestUpdateEventComment(t *testing.T) {
	eventCommentMap := make(map[string]bpdb.EventComment)
	backend := test.NewMockBpEventCommentBackend(eventCommentMap)
	configFile := createJSONFile(t, "TestUpdateEventComment")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, backend, nil, configFile.Name(), nil, "", false).(*server)
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"username": "", "event": "this-table-exists"},
	}

	cfg := scoop.EventComment{EventName: "this-table-exists", EventComment: "Test Comment", UserName: ""}
	cfgBytes, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal("unable to marshal scoop config, bailing")
	}

	createRecorder := httptest.NewRecorder()
	createReq, _ := http.NewRequest("PUT", "/comment/this-table-exists", bytes.NewReader(cfgBytes))

	s.updateEventComment(c, createRecorder, createReq)
	assertRequestOK(t, "TestUpdateEventComment", createRecorder, "")
}

// Tests trying to get metadata for an event with no schema
// Expected result is a 500 internal error
func TestGetEventMetadataNotFound(t *testing.T) {
	eventMetadataMap := make(map[string]bpdb.EventMetadata)
	backend := test.NewMockBpEventMetadataBackend(eventMetadataMap)
	configFile := createJSONFile(t, "TestGetEventMetadataNotFound")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, nil, backend, configFile.Name(), nil, "", false).(*server)
	recorder := httptest.NewRecorder()
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"username": "", "event": "this-table-does-not-exist"},
	}
	req, _ := http.NewRequest("GET", "/metadata/this-table-does-not-exist", nil)
	s.eventMetadata(c, recorder, req)

	expectedErrorMsg := "no metadata found for event this-table-does-not-exist"
	assertRequestInternalError(t, "TestGetEventMetadataNotFound", recorder, expectedErrorMsg)
}

// Tests trying to get metadata for an event with a schema
// Expected result is a 200 OK response
func TestGetEventMetadata(t *testing.T) {
	eventMetadataMap := make(map[string]bpdb.EventMetadata)
	eventMetadataMap["this-table-exists"] = bpdb.EventMetadata{
		EventName: "event",
		Metadata: []bpdb.EventMetadataRow{
			bpdb.EventMetadataRow{
				MetadataType:  "comment",
				MetadataValue: "Test comment",
				UserName:      "legacy",
				Version:       2,
			},
		},
	}
	backend := test.NewMockBpEventMetadataBackend(eventMetadataMap)
	configFile := createJSONFile(t, "TestGetEventMetadata")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, nil, backend, configFile.Name(), nil, "", false).(*server)
	recorder := httptest.NewRecorder()
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"username": "", "event": "this-table-exists"},
	}
	req, _ := http.NewRequest("GET", "/metadata/this-table-exists", nil)

	s.eventMetadata(c, recorder, req)
	expectedBody := "[{\"EventName\":\"event\",\"Metadata\":[{\"MetadataType\":\"comment\",\"MetadataValue\":" +
		"\"Test comment\",\"TS\":\"0001-01-01T00:00:00Z\",\"UserName\":\"legacy\",\"Version\":2}]}]"
	assertRequestOK(t, "TestGetEventMetadata", recorder, expectedBody)
}

// Tests trying to update metadata for an event with no schema
// Expected result is a 400 bad request
func TestUpdateEventMetadataNoSchema(t *testing.T) {
	eventMetadataMap := make(map[string]bpdb.EventMetadata)
	eventMetadataMap["this-table-does-not-exist"] = bpdb.EventMetadata{}
	backend := test.NewMockBpEventMetadataBackend(eventMetadataMap)
	configFile := createJSONFile(t, "TestUpdateEventMetadataNoSchema")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, nil, backend, configFile.Name(), nil, "", false).(*server)
	c := web.C{
		Env:       map[interface{}]interface{}{"username": ""},
		URLParams: map[string]string{"username": "", "event": "this-table-does-not-exist"},
	}

	cfg := core.ClientUpdateEventMetadataRequest{EventName: "this-table-does-not-exist", MetadataType: "comment", MetadataValue: "Test comment"}
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

	s := New("", nil, nil, nil, nil, backend, configFile.Name(), nil, "", false).(*server)
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
	assertRequestInternalError(t, "TestUpdateEventMetadataInvalidMetadataType", recorder, "Internal error: Invalid event metadata type")
}

// Tests trying to update metadata for an event with a schema
// Expected result is a 200 OK response
func TestUpdateEventMetadata(t *testing.T) {
	eventMetadataMap := make(map[string]bpdb.EventMetadata)
	backend := test.NewMockBpEventMetadataBackend(eventMetadataMap)
	configFile := createJSONFile(t, "TestUpdateEventMetadata")

	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, nil, backend, configFile.Name(), nil, "", false).(*server)
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

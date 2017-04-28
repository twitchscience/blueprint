package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/blueprint/test"
	scoop "github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/zenazn/goji/web"
)

func TestMigrationNegativeTo(t *testing.T) {
	configFile := createJSONFile(t, "testMigration")
	defer deleteJSONFile(t, configFile)
	writeConfig(t, configFile)

	s := New("", nil, nil, nil, configFile.Name(), nil, "", false).(*server)
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

	s := New("", nil, backend, nil, configFile.Name(), nil, "", false).(*server)
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
		assertRequestOK(t, "allSchemas", getAllRecorder)
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
	assertRequestOK(t, "createSchema", createRecorder)
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
	assertRequestOK(t, "updateSchema", updateRecorder)
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

func assertRequestOK(t *testing.T, testedName string, w *httptest.ResponseRecorder) {
	if status := w.Code; status != http.StatusOK {
		t.Errorf("%v returned status code %v, want %v", testedName, status, http.StatusOK)
	}
}

func printTotalAllSchemasCalls(t *testing.T, backend *test.MockBpSchemaBackend) {
	t.Logf("AllSchemas() calls seen: %v", backend.GetAllSchemasCalls())
}

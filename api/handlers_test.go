package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/zenazn/goji/web"
)

func TestMigrationNegativeTo(t *testing.T) {
	s := New("", nil, "", nil, "").(*server)
	handler := web.HandlerFunc(s.migration)
	recorder := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/migration/testerino?to_version=-4", nil)
	handler.ServeHTTP(recorder, req)
	if status := recorder.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}
}

package api

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/twitchscience/blueprint/auth"
	"github.com/twitchscience/blueprint/core"
	cachingscoopclient "github.com/twitchscience/blueprint/scoopclient/cachingclient"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"

	"github.com/zenazn/goji/web"
)

// respondWithJsonError responds with a JSON error with the given error code. The format of the
// JSON error is {"Error": text}
// It's very likely that you want to return from the handler after calling
// this.
func respondWithJsonError(w http.ResponseWriter, text string, responseCode int) {

	var jsonError struct {
		Error string
	}
	jsonError.Error = text
	js, err := json.Marshal(jsonError)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(responseCode)
	w.Write(js)
}

// ingest proxies the request through to the ingester /control/ingest
func (s *server) ingest(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var tableArg struct {
		Table string
	}
	err := decoder.Decode(&tableArg)
	if err != nil {
		respondWithJsonError(w, "Problem decoding JSON POST data.", http.StatusBadRequest)
		return
	}

	a := auth.New(githubServer,
		clientID,
		clientSecret,
		cookieSecret,
		requiredOrg,
		loginURL)
	user := a.User(r)
	log.Printf("%s requested table %s be flushed.", user.Name, tableArg.Table)

	js, err := json.Marshal(tableArg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	req, err := http.NewRequest("POST", ingesterURL+"/control/ingest", bytes.NewBuffer(js))
	if err != nil {
		respondWithJsonError(w, "Error building request to ingester: "+err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		respondWithJsonError(w, "Error making request to ingester: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	w.Write(buf.Bytes())
	return
}

func (s *server) createSchema(c web.C, w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	var cfg scoop_protocol.Config
	err = json.Unmarshal(b, &cfg)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if err := s.datasource.CreateSchema(&cfg); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
}

func (s *server) updateSchema(c web.C, w http.ResponseWriter, r *http.Request) {
	// TODO: when refactoring the front end do not send the event name
	// since it should be infered from the url
	eventName := c.URLParams["id"]

	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	var req core.ClientUpdateSchemaRequest
	err = json.Unmarshal(b, &req)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	req.EventName = eventName

	if err := s.datasource.UpdateSchema(&req); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
}

func (s *server) allSchemas(w http.ResponseWriter, r *http.Request) {
	cfgs, err := s.datasource.FetchAllSchemas()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeEvent(w, cfgs)
}

func (s *server) schema(c web.C, w http.ResponseWriter, r *http.Request) {
	cfg, err := s.datasource.FetchSchema(c.URLParams["id"])
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if cfg == nil {
		fourOhFour(w, r)
		return
	}
	writeEvent(w, []scoop_protocol.Config{*cfg})
}

func (s *server) fileHandler(w http.ResponseWriter, r *http.Request) {
	fh, err := os.Open(staticPath(s.docRoot, r.URL.Path))
	if err != nil {
		fourOhFour(w, r)
		return
	}
	io.Copy(w, fh)
}

func (s *server) types(w http.ResponseWriter, r *http.Request) {
	props, err := s.datasource.PropertyTypes()
	if err != nil {
		http.Error(w, err.Error(), 500)
	}
	data := make(map[string][]string)
	data["result"] = props
	b, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Write(b)
}

func (s *server) expire(w http.ResponseWriter, r *http.Request) {
	if v := s.datasource.(*cachingscoopclient.CachingClient); v != nil {
		v.Expire()
	}
}

func (s *server) listSuggestions(w http.ResponseWriter, r *http.Request) {
	availableSuggestions, err := getAvailableSuggestions(s.docRoot)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	if len(availableSuggestions) == 0 {
		w.Write([]byte("[]"))
		return
	}

	b, err := json.Marshal(availableSuggestions)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Write(b)
}

func (s *server) suggestion(c web.C, w http.ResponseWriter, r *http.Request) {
	if !validSuggestion(strings.TrimSuffix(c.URLParams["id"], ".json"), s.docRoot) {
		fourOhFour(w, r)
		return
	}
	fh, err := os.Open(s.docRoot + "/events/" + c.URLParams["id"])
	if err != nil {
		fourOhFour(w, r)
		return
	}
	io.Copy(w, fh)
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
	io.WriteString(w, "Healthy")
}

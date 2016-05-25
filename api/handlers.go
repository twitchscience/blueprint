package api

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/twitchscience/blueprint/auth"
	"github.com/twitchscience/blueprint/core"
	cachingscoopclient "github.com/twitchscience/blueprint/scoopclient/cachingclient"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"

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
		log.Printf("Error marshalling JSON: %v.", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(responseCode)
	_, err = w.Write(js)
	if err != nil {
		log.Printf("Error writing JSON to response: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ingest proxies the request through to the ingester /control/ingest
func (s *server) ingest(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var tableArg struct {
		Table string
	}
	err := decoder.Decode(&tableArg)
	if err != nil {
		respondWithJSONError(w, "Problem decoding JSON POST data.", http.StatusBadRequest)
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
		log.Printf("Error marshalling JSON: %v.", err)
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
			log.Printf("Error closing response body: %v.", err)
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	if err != nil {
		log.Printf("Error writing to response: %v.", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = w.Write(buf.Bytes())
	if err != nil {
		log.Printf("Error writing to response: %v.", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	return
}

func (s *server) createSchema(c web.C, w http.ResponseWriter, r *http.Request) {
	defer func() {
		err := r.Body.Close()
		if err != nil {
			log.Printf("Error closing request body: %v.", err)
		}
	}()

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
	err = s.datasource.CreateSchema(&cfg)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	err = s.bpdbBackend.CreateSchema(&cfg)
	if err != nil {
		log.Printf("Error creating schema in bpdb, ignoring: %v", err)
	}
}

func (s *server) updateSchema(c web.C, w http.ResponseWriter, r *http.Request) {
	// TODO: when refactoring the front end do not send the event name
	// since it should be infered from the url
	eventName := c.URLParams["id"]

	defer func() {
		err := r.Body.Close()
		if err != nil {
			log.Printf("Error closing request body: %v.", err)
		}
	}()

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

	err = s.datasource.UpdateSchema(&req)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	err = s.bpdbBackend.UpdateSchema(&req)
	if err != nil {
		log.Printf("Error updating schema in bpdb, ignoring: %v", err)
	}
}

func (s *server) allSchemas(w http.ResponseWriter, r *http.Request) {
	cfgs, err := s.bpdbBackend.AllSchemas()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeEvent(w, cfgs)
}

func (s *server) schema(c web.C, w http.ResponseWriter, r *http.Request) {
	cfg, err := s.bpdbBackend.Schema(c.URLParams["id"])
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if cfg == nil {
		fourOhFour(w, r)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeEvent(w, []scoop_protocol.Config{*cfg})
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
		log.Printf("Error copying file %s to response: %v.", fname, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
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
	_, err = w.Write(b)
	if err != nil {
		log.Printf("Error writing to response: %v.", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) expire(w http.ResponseWriter, r *http.Request) {
	v := s.datasource.(*cachingscoopclient.CachingClient)
	if v != nil {
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
		_, err = w.Write([]byte("[]"))
		if err != nil {
			log.Printf("Error writing to response: %v.", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	b, err := json.Marshal(availableSuggestions)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	_, err = w.Write(b)
	if err != nil {
		log.Printf("Error writing to response: %v.", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
		log.Printf("Error copying file %s to response: %v.", fname, err)
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
		log.Printf("Error writing to response: %v.", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

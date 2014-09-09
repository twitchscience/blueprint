package api

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/twitchscience/blueprint/core"
	cachingscoopclient "github.com/twitchscience/blueprint/scoopclient/cachingclient"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"

	"github.com/zenazn/goji/web"
)

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
	fh, err := os.Open(path(s.docRoot, r.URL.Path))
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
	availableSuggestions := make([]string, 0)
	filepath.Walk(s.docRoot+"/events", func(path string, info os.FileInfo, err error) error {
		if path == s.docRoot+"/events" {
			return nil
		}
		if info.IsDir() {
			return filepath.SkipDir
		}
		eventNameIdx := strings.Index(info.Name(), ".")
		if eventNameIdx > 0 && info.Name()[eventNameIdx:len(info.Name())] == ".json" {
			availableSuggestions = append(availableSuggestions, info.Name())
		}
		return nil
	})
	b, err := json.Marshal(availableSuggestions)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Write(b)
}

func (s *server) suggestion(c web.C, w http.ResponseWriter, r *http.Request) {
	fh, err := os.Open(s.docRoot + "/events/" + c.URLParams["id"])
	if err != nil {
		fourOhFour(w, r)
		return
	}
	io.Copy(w, fh)
}

func (s *server) removeSuggestion(c web.C, w http.ResponseWriter, r *http.Request) {
	err := os.Remove(s.docRoot + "/events/" + c.URLParams["id"])
	if err != nil {
		fourOhFour(w, r)
		return
	}
}

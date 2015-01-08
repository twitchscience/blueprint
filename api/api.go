// Package api exposes the scoop HTTP API. Scoop manages the state of tables in Redshift.
package api

import (
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/blueprint/scoopclient"
	"github.com/zenazn/goji"
	"github.com/zenazn/goji/graceful"
	"github.com/zenazn/goji/web"
)

type server struct {
	docRoot    string
	datasource scoopclient.ScoopClient
}

// New returns an API process.
func New(docRoot string, client scoopclient.ScoopClient) core.Subprocess {
	return &server{
		docRoot:    docRoot,
		datasource: client,
	}
}

// Setup route handlers.
func (s *server) Setup() error {
	files := web.New()
	files.Get("/*", s.fileHandler)
	files.NotFound(fourOhFour)

	api := web.New()
	api.Use(jsonResponse)
	api.Put("/schema", s.createSchema)
	api.Get("/schemas", s.allSchemas)
	api.Get("/schema/:id", s.schema)
	api.Post("/schema/:id", s.updateSchema)
	api.Get("/types", s.types)
	api.Post("/expire", s.expire)
	api.Get("/suggestions", s.listSuggestions)
	api.Get("/suggestion/:id", s.suggestion)
	api.Post("/removesuggestion/:id", s.removeSuggestion)

	// Order is important here
	goji.Handle("/schema*", api)
	goji.Handle("/suggestion*", api)
	goji.Handle("/types", api)
	goji.Handle("/expire", api)
	goji.Handle("/*", files)

	// Stop() provides our shutdown semantics
	graceful.ResetSignals()

	return nil
}

// Start the API server.
func (s *server) Start() {
	goji.Serve()
}

// Stop the API server.
func (s *server) Stop() {
	graceful.Shutdown()
}

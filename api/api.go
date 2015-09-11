// Package api exposes the scoop HTTP API. Scoop manages the state of tables in Redshift.
package api

import (
	"flag"

	"github.com/twitchscience/blueprint/auth"
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

var (
	loginURL        = "/login"
	logoutURL       = "/logout"
	authCallbackURL = "/github_oauth_cb"
	readonly        bool
	cookieSecret    string
	clientID        string
	clientSecret    string
	githubServer    string
	requiredOrg     string
)

func init() {
	flag.BoolVar(&readonly, "readonly", false, "run in readonly mode and disable auth")
	flag.StringVar(&cookieSecret, "cookieSecret", "", "32 character secret for signing cookies")
	flag.StringVar(&clientID, "clientID", "", "Google API client id")
	flag.StringVar(&clientSecret, "clientSecret", "", "Google API client secret")
	flag.StringVar(&githubServer, "githubServer", "http://github.com", "Github server to use for auth")
	flag.StringVar(&requiredOrg, "requiredOrg", "", "Org user need to belong to to use auth")
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
	healthcheck := web.New()
	healthcheck.Get("/health", s.healthCheck)

	api := web.New()
	api.Use(jsonResponse)
	api.Get("/schemas", s.allSchemas)
	api.Get("/schema/:id", s.schema)
	api.Get("/types", s.types)
	api.Get("/suggestions", s.listSuggestions)
	api.Get("/suggestion/:id", s.suggestion)

	goji.Handle("/health", healthcheck)
	goji.Handle("/schemas", api)
	goji.Handle("/schema/*", api)
	goji.Handle("/suggestions", api)
	goji.Handle("/suggestion/*", api)
	goji.Handle("/types", api)
	goji.Handle("/expire", api)

	if !readonly {
		a := auth.New(githubServer,
			clientID,
			clientSecret,
			cookieSecret,
			requiredOrg,
			loginURL)

		api.Use(a.UserMiddleware)

		api.Put("/schema", s.createSchema)
		api.Post("/expire", s.expire)
		api.Post("/schema/:id", s.updateSchema)
		api.Post("/removesuggestion/:id", s.removeSuggestion)

		goji.Handle(loginURL, a.LoginHandler)
		goji.Handle(logoutURL, a.LogoutHandler)
		goji.Handle(authCallbackURL, a.AuthCallbackHandler)

		files := web.New()
		files.Get("/*", s.fileHandler)
		files.Use(a.UserMiddleware)

		goji.Handle("/*", files)
	}
	goji.NotFound(fourOhFour)

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

package api

import (
	"flag"
	"regexp"
	"time"

	"github.com/gorilla/context"
	gzip "github.com/lidashuang/goji-gzip"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/blueprint/auth"
	"github.com/twitchscience/blueprint/bpdb"
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/blueprint/ingester"
	"github.com/zenazn/goji"
	"github.com/zenazn/goji/graceful"
	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"
)

type schemaResult struct {
	allSchemas []bpdb.AnnotatedSchema
	err        error
}

type server struct {
	docRoot            string
	bpdbBackend        bpdb.Bpdb
	configFilename     string
	ingesterController ingester.Controller
	slackbotURL        string
	cacheSynchronizer  chan func()
	cachedResult       *schemaResult
	cachedVersion      int
	cacheTimeout       time.Duration
	blacklistRe        []*regexp.Regexp
}

var (
	loginURL           = "/login"
	logoutURL          = "/logout"
	authCallbackURL    = "/github_oauth_cb"
	slackbotDeletePath = "/request-table-delete"
	enableAuth         bool
	readonly           bool
	cookieSecret       string
	clientID           string
	clientSecret       string
	githubServer       string
	requiredOrg        string
)

func init() {
	flag.BoolVar(&enableAuth, "enableAuth", true, "enable authentication when not in readonly mode")
	flag.BoolVar(&readonly, "readonly", false, "run in readonly mode and disable auth")
	flag.StringVar(&cookieSecret, "cookieSecret", "", "32 character secret for signing cookies")
	flag.StringVar(&clientID, "clientID", "", "Google API client id")
	flag.StringVar(&clientSecret, "clientSecret", "", "Google API client secret")
	flag.StringVar(&githubServer, "githubServer", "http://github.com", "Github server to use for auth")
	flag.StringVar(&requiredOrg, "requiredOrg", "", "Org user need to belong to to use auth")
}

// New returns an API process.
func New(docRoot string, bpdbBackend bpdb.Bpdb, configFilename string, ingCont ingester.Controller, slackbotURL string) core.Subprocess {
	s := &server{
		docRoot:            docRoot,
		bpdbBackend:        bpdbBackend,
		configFilename:     configFilename,
		ingesterController: ingCont,
		slackbotURL:        slackbotURL,
		cacheSynchronizer:  make(chan func()),
		cachedResult:       nil,
		cachedVersion:      0,
	}
	if err := s.loadConfig(); err != nil {
		logger.WithError(err).Fatal("failed to load config")
	}
	logger.Go(func() {
		for f := range s.cacheSynchronizer {
			f()
		}
	})
	return s
}

// Setup route handlers.
func (s *server) Setup() error {
	healthcheck := web.New()
	healthcheck.Get("/health", s.healthCheck)

	roAPI := web.New()
	roAPI.Use(jsonResponse)
	roAPI.Use(gzip.GzipHandler)
	roAPI.Get("/schemas", s.allSchemas)
	roAPI.Get("/schema/:id", s.schema)
	roAPI.Get("/droppable/schema/:id", s.droppableSchema)
	roAPI.Get("/migration/:schema", s.migration)
	roAPI.Get("/types", s.types)
	roAPI.Get("/suggestions", s.listSuggestions)
	roAPI.Get("/suggestion/:id", s.suggestion)

	goji.Get("/health", healthcheck)
	goji.Get("/schemas", roAPI)
	goji.Get("/schema/*", roAPI)
	goji.Get("/droppable/schema/*", roAPI)
	goji.Get("/migration/*", roAPI)
	goji.Get("/suggestions", roAPI)
	goji.Get("/suggestion/*", roAPI)
	goji.Get("/types", roAPI)

	if !readonly {
		api := web.New()
		api.Use(context.ClearHandler)

		api.Post("/ingest", s.ingest)
		api.Put("/schema", s.createSchema)
		api.Post("/schema/:id", s.updateSchema)
		api.Post("/drop/schema", s.dropSchema)
		api.Post("/removesuggestion/:id", s.removeSuggestion)

		goji.Post("/ingest", api)
		goji.Put("/schema", api)
		goji.Post("/schema/*", api)
		goji.Post("/drop/schema", api)
		goji.Post("/removesuggestion/*", api)

		files := web.New()
		files.Use(context.ClearHandler)

		a := auth.New(githubServer,
			clientID,
			clientSecret,
			cookieSecret,
			requiredOrg,
			loginURL)

		if enableAuth {
			api.Use(a.AuthorizeOrForbid)

			goji.Handle(loginURL, a.LoginHandler)
			goji.Handle(logoutURL, a.LogoutHandler)
			goji.Handle(authCallbackURL, a.AuthCallbackHandler)
			files.Use(a.ExpireDisplayName)
		} else {
			api.Use(auth.DummyAuth)
			goji.Handle(loginURL, auth.DummyLoginHandler)
			goji.Handle(logoutURL, auth.DummyLogoutHandler)
		}

		goji.Handle("/*", files)
		files.Get("/*", s.fileHandler)
	}
	goji.NotFound(fourOhFour)

	// The default logger logs in colour which makes CloudWatch hard to read.
	// Replace with a custom logger that does not use colour.
	err := goji.DefaultMux.Abandon(middleware.Logger)
	if err != nil {
		logger.WithError(err).Warn("Couldn't abandon default logger; will continue as is")
	} else {
		goji.DefaultMux.Use(SimpleLogger)
	}

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

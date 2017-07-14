package api

import (
	"flag"
	"io"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	// "github.com/aws/aws-sdk-go/service/s3/s3manageriface"
	"github.com/gorilla/context"
	gzip "github.com/lidashuang/goji-gzip"
	"github.com/patrickmn/go-cache"
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

const (
	allSchemasCache  = "allSchemas"
	allMetadataCache = "allMetadata"
)

type server struct {
	docRoot                string
	bpdbBackend            bpdb.Bpdb
	bpSchemaBackend        bpdb.BpSchemaBackend
	bpKinesisConfigBackend bpdb.BpKinesisConfigBackend
	bpEventMetadataBackend bpdb.BpEventMetadataBackend
	configFilename         string
	ingesterController     ingester.Controller
	slackbotURL            string
	goCache                *cache.Cache
	cacheTimeout           time.Duration
	blacklistRe            []*regexp.Regexp
	readonly               bool
	s3Uploader             s3manageriface.UploaderAPI
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
	adminTeam          string
)

// S3UploaderWrapper tests stuff
type S3UploaderWrapper struct {
	s3manageriface.UploaderAPI
}

// S3DownloaderWrapper tests stuff
type S3DownloaderWrapper struct {
	s3manageriface.DownloaderAPI
}

// S3ManagerMockInterface is mock interface for the S3Manager
type S3ManagerMockInterface struct {
	s3manageriface.UploaderAPI
	s3manageriface.DownloaderAPI
}

func init() {
	flag.BoolVar(&enableAuth, "enableAuth", true, "enable authentication when not in readonly mode")
	flag.StringVar(&cookieSecret, "cookieSecret", "", "32 character secret for signing cookies")
	flag.StringVar(&clientID, "clientID", "", "Google API client id")
	flag.StringVar(&clientSecret, "clientSecret", "", "Google API client secret")
	flag.StringVar(&githubServer, "githubServer", "http://github.com", "Github server to use for auth")
	flag.StringVar(&requiredOrg, "requiredOrg", "", "Org user need to belong to to use auth")
	flag.StringVar(&adminTeam, "adminTeam", "", "Team with admin privileges")
}

// New returns an API process.
func New(
	docRoot string,
	bpdbBackend bpdb.Bpdb,
	bpSchemaBackend bpdb.BpSchemaBackend,
	bpKinesisConfigBackend bpdb.BpKinesisConfigBackend,
	bpEventMetadataBackend bpdb.BpEventMetadataBackend,
	configFilename string,
	ingCont ingester.Controller,
	slackbotURL string,
	readonly bool,
	s3Uploader s3manageriface.UploaderAPI,
) core.Subprocess {
	s := &server{
		docRoot:                docRoot,
		bpdbBackend:            bpdbBackend,
		bpSchemaBackend:        bpSchemaBackend,
		bpEventMetadataBackend: bpEventMetadataBackend,
		bpKinesisConfigBackend: bpKinesisConfigBackend,
		configFilename:         configFilename,
		ingesterController:     ingCont,
		slackbotURL:            slackbotURL,
		goCache:                cache.New(5*time.Minute, 10*time.Minute),
		readonly:               readonly,
		s3Uploader:             s3Uploader,
	}
	if err := s.loadConfig(); err != nil {
		logger.WithError(err).Fatal("failed to load config")
	}
	return s
}

// NewS3Uploader returns a new S3Uploader
func NewS3Uploader() *s3manager.Uploader {
	s := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-west-2"),
	}))
	return s3manager.NewUploader(s)
}

func NewMockS3Uploader() *S3UploaderWrapper {
	// s := session.Must(session.NewSession(&aws.Config{
	// 	Region: aws.String("us-west-2"),
	// }))

	// return s3manager.NewUploader(s)
	return &S3UploaderWrapper{}
}

func (s *S3DownloaderWrapper) Download(io.WriterAt, *s3.GetObjectInput, ...func(*s3manager.Downloader)) (int64, error) {
	return 0, nil
}

func (s *S3UploaderWrapper) Upload(*s3manager.UploadInput, ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	return &s3manager.UploadOutput{}, nil
}

// Create a simple health check API which needs no special setup.
func (s *server) setupHealthcheckAPI() {
	healthcheckAPI := web.New()
	healthcheckAPI.Get("/health", s.healthCheck)
	goji.Get("/health", healthcheckAPI)
}

// Create the read-only API, available to all users.
func (s *server) setupReadonlyAPI() {
	roAPI := web.New()
	roAPI.Use(jsonResponse)
	roAPI.Use(gzip.GzipHandler)

	roAPI.Get("/schemas", s.allSchemas)
	roAPI.Get("/schema/:id", s.schema)
	roAPI.Get("/droppable/schema/:id", s.droppableSchema)
	roAPI.Get("/maintenance", s.getMaintenanceMode)
	roAPI.Get("/maintenance/:schema", s.getMaintenanceMode)
	roAPI.Get("/migration/:schema", s.migration)
	roAPI.Get("/types", s.types)
	roAPI.Get("/suggestions", s.listSuggestions)
	roAPI.Get("/suggestion/:id", s.suggestion)
	roAPI.Get("/stats", s.stats)
	roAPI.Get("/allmetadata", s.allEventMetadata)
	roAPI.Get("/metadata/:event", s.eventMetadata)

	goji.Get("/schemas", roAPI)
	goji.Get("/schema/*", roAPI)
	goji.Get("/droppable/schema/*", roAPI)
	goji.Get("/maintenance", roAPI)
	goji.Get("/maintenance/*", roAPI)
	goji.Get("/migration/*", roAPI)
	goji.Get("/types", roAPI)
	goji.Get("/suggestions", roAPI)
	goji.Get("/suggestion/*", roAPI)
	goji.Get("/stats", roAPI)
	goji.Get("/allmetadata", roAPI)
	goji.Get("/metadata/*", roAPI)

	roAPI.Get("/kinesisconfigs", s.allKinesisConfigs)
	roAPI.Get("/kinesisconfig/:account/:type/:name", s.kinesisconfig)
	goji.Get("/kinesisconfigs", roAPI)
	goji.Get("/kinesisconfig/*", roAPI)
}

// Create the write API available only to authenticated users, which includes creating and
// adding rows to schemata, requesting deletion, and starting force loads.  Because it
// involves changes to the Blueprint DB, all of it is locked down during maintenance mode.
func (s *server) authWriteAPI() *web.Mux {
	authWriteAPI := web.New()
	authWriteAPI.Use(context.ClearHandler)
	authWriteAPI.Use(s.maintenanceHandler)

	authWriteAPI.Post("/force_load", s.forceLoad)
	authWriteAPI.Put("/schema", s.createSchema)
	authWriteAPI.Post("/schema/:id", s.updateSchema)
	authWriteAPI.Post("/drop/schema", s.dropSchema)
	authWriteAPI.Post("/removesuggestion/:id", s.removeSuggestion)
	authWriteAPI.Post("/metadata/:event", s.updateEventMetadata)

	goji.Post("/force_load", authWriteAPI)
	goji.Put("/schema", authWriteAPI)
	goji.Post("/schema/*", authWriteAPI)
	goji.Post("/drop/schema", authWriteAPI)
	goji.Post("/removesuggestion/*", authWriteAPI)
	goji.Post("/metadata/*", authWriteAPI)

	return authWriteAPI
}

// Create the write API available only to admins. Currently limited to toggling maintenance
// modes and modifying Kinesis configs.
func (s *server) authAdminAPI() *web.Mux {
	adminAPI := web.New()
	adminAPI.Use(context.ClearHandler)

	adminAPI.Post("/maintenance", s.setMaintenanceMode)
	adminAPI.Post("/maintenance/:schema", s.setMaintenanceMode)
	goji.Post("/maintenance", adminAPI)
	goji.Post("/maintenance/*", adminAPI)

	adminAPI.Put("/kinesisconfig", s.createKinesisConfig)
	adminAPI.Post("/kinesisconfig/:account/:type/:name", s.updateKinesisConfig)
	adminAPI.Post("/drop/kinesisconfig", s.dropKinesisConfig)
	goji.Put("/kinesisconfig", adminAPI)
	goji.Post("/kinesisconfig/*", adminAPI)
	goji.Post("/drop/kinesisconfig", adminAPI)

	return adminAPI
}

// Set up the authenticated portion of the API.
func (s *server) setupAuthAPI() {
	authWriteAPI := s.authWriteAPI()
	adminAPI := s.authAdminAPI()

	files := web.New()
	files.Use(context.ClearHandler)

	if enableAuth {
		a := auth.New(githubServer,
			clientID,
			clientSecret,
			cookieSecret,
			requiredOrg,
			adminTeam,
			loginURL)

		authWriteAPI.Use(a.AuthorizeOrForbid)
		adminAPI.Use(a.AuthorizeOrForbidAdmin)
		goji.Handle(loginURL, a.LoginHandler)
		goji.Handle(logoutURL, a.LogoutHandler)
		goji.Handle(authCallbackURL, a.AuthCallbackHandler)
		files.Use(a.ExpireDisplayName)
	} else {
		authWriteAPI.Use(auth.DummyAuth)
		adminAPI.Use(auth.DummyAuth)
		goji.Handle(loginURL, auth.DummyLoginHandler)
		goji.Handle(logoutURL, auth.DummyLogoutHandler)
	}

	goji.Handle("/*", files)
	files.Get("/*", s.fileHandler)
}

// Setup route handlers.
func (s *server) Setup() error {
	s.setupHealthcheckAPI()
	s.setupReadonlyAPI()

	if !s.readonly {
		s.setupAuthAPI()
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

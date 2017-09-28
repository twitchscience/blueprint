/*
Package blueprint provides an API server and UI for editing the Spade
processing rules. Each schema defines processing rules for a particular event
type, and results in that event being ingested by rs_ingester into Redshift.
Processing rules transform one or more input fields to a TSV-compatible
string. Systems can use the read- only endpoints to fetch the schema data
without authentication. It listens on port 8080 for unauthenticated HTTP
requests and on port 8081 for authenticated HTTP requests (assuming an Elastic
Load Balancer already performed SSL termination).
*/
package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"net/http"
	_ "net/http/pprof"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/blueprint/api"
	"github.com/twitchscience/blueprint/bpdb"
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/blueprint/ingester"
)

var (
	bpdbConnection     = flag.String("bpdbConnection", "", "The connection string for blueprintdb")
	staticFileDir      = flag.String("staticfiles", "./static", "the location to serve static files from")
	configFilename     = flag.String("config", "conf.json", "Blueprint config file")
	ingesterURL        = flag.String("ingesterURL", "", "URL to the ingester")
	slackbotURL        = flag.String("slackbotURL", "", "URL for the slackbot")
	readonly           = flag.Bool("readonly", false, "run in readonly mode and disable auth")
	rollbarToken       = flag.String("rollbarToken", "", "Rollbar post_server_item token")
	rollbarEnvironment = flag.String("rollbarEnvironment", "", "Rollbar environment")
)

type config struct {
	APIConfig      api.Config `json:"apiConfig"`
	KinesisFilters []string   `json:"kinesisFilters"`
}

func loadConfig(filename string) (*config, error) {
	configJSON, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var conf config
	if err := json.Unmarshal(configJSON, &conf); err != nil {
		return nil, err
	}

	return &conf, nil
}

func main() {
	flag.Parse()

	logger.InitWithRollbar("info", *rollbarToken, *rollbarEnvironment)
	logger.CaptureDefault()
	logger.Info("Starting!")
	defer logger.LogPanic()

	conf, err := loadConfig(*configFilename)
	if err != nil {
		logger.WithError(err).Fatal("Failed loading config")
	}

	logger.Go(func() {
		port := ":7766"
		if *readonly {
			port = ":7767"
		}
		logger.WithError(http.ListenAndServe(port, nil)).Error("Serving pprof failed")
	})

	db, err := sql.Open("postgres", *bpdbConnection)
	if err != nil {
		logger.WithError(err).Fatal("Failed to connect to DB")
	}

	// Set up 3 backend objects; they handle schema actions, kinesis actions, and maintenance/stats actions
	bpdbBackend, err := bpdb.NewPostgresBackend(db)
	if err != nil {
		logger.WithError(err).Fatal("Error setting up blueprint db backend")
	}
	bpSchemaBackend, err := bpdb.NewSchemaBackend(db)
	if err != nil {
		logger.WithError(err).Fatal("Error setting up blueprint schema backend")
	}
	bpKinesisConfigBackend := bpdb.NewKinesisConfigBackend(db, conf.KinesisFilters)

	ingCont := ingester.NewController(*ingesterURL)

	apiProcess := api.New(*staticFileDir, bpdbBackend, bpSchemaBackend, bpKinesisConfigBackend,
		&conf.APIConfig, ingCont, *slackbotURL, *readonly, api.NewS3Uploader())
	manager := &core.SubprocessManager{
		Processes: []core.Subprocess{
			apiProcess,
		},
	}
	manager.Start()

	shutdownSignal := make(chan os.Signal)
	signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)
	logger.Go(func() {
		<-shutdownSignal
		logger.Info("Sigint received -- shutting down")
		manager.Stop()
	})

	manager.Wait()
	logger.Info("Exiting main cleanly.")
	logger.Wait()
}

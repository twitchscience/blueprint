package main

import (
	"flag"
	"os"
	"os/signal"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/blueprint/api"
	"github.com/twitchscience/blueprint/bpdb"
	"github.com/twitchscience/blueprint/core"
)

var (
	bpdbConnection     = flag.String("bpdbConnection", "", "The connection string for blueprintdb")
	staticFileDir      = flag.String("staticfiles", "./static", "the location to serve static files from")
	configFilename     = flag.String("config", "conf.json", "Blueprint config file")
	rollbarToken       = flag.String("rollbarToken", "", "Rollbar post_server_item token")
	rollbarEnvironment = flag.String("rollbarEnvironment", "", "Rollbar environment")
)

func main() {
	flag.Parse()

	logger.InitWithRollbar("info", *rollbarToken, *rollbarEnvironment)
	logger.CaptureDefault()
	logger.Info("Starting!")
	defer logger.LogPanic()

	bpdbBackend, err := bpdb.NewPostgresBackend(*bpdbConnection)
	if err != nil {
		logger.WithError(err).Fatal("Error setting up blueprint db backend")
	}

	apiProcess := api.New(*staticFileDir, bpdbBackend, *configFilename)
	manager := &core.SubprocessManager{
		Processes: []core.Subprocess{
			apiProcess,
		},
	}
	manager.Start()

	shutdownSignal := make(chan os.Signal)
	signal.Notify(shutdownSignal)
	logger.Go(func() {
		<-shutdownSignal
		logger.Info("Sigint received -- shutting down")
		manager.Stop()
	})

	manager.Wait()
	logger.Info("Exiting main cleanly.")
	logger.Wait()
}

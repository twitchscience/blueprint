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
	bpdbConnection = flag.String("bpdbConnection", "", "The connection string for blueprintdb")
	staticFileDir  = flag.String("staticfiles", "./static", "the location to serve static files from")
	configFilename = flag.String("config", "conf.json", "Blueprint config file")
)

func main() {
	flag.Parse()
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
	go func() {
		<-shutdownSignal
		manager.Stop()
	}()

	manager.Wait()
}

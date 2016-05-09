package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/twitchscience/blueprint/api"
	"github.com/twitchscience/blueprint/bpdb"
	"github.com/twitchscience/blueprint/core"
	cachingscoopclient "github.com/twitchscience/blueprint/scoopclient/cachingclient"
)

var (
	scoopURL        = flag.String("scoopURL", "", "the base url for scoop")
	bpdbConnection  = flag.String("bpdbConnection", "", "The connection string for blueprintdb")
	staticFileDir   = flag.String("staticfiles", "./static", "the location to serve static files from")
	transformConfig = flag.String("transformConfig", "transforms_available.json", "config for available transforms in spade")
)

func main() {
	flag.Parse()
	scoopClient := cachingscoopclient.New(*scoopURL, *transformConfig)
	bpdbBackend, err := bpdb.NewPostgresBackend(*bpdbConnection)
	if err != nil {
		log.Fatalf("Error setting up blueprint db backend: %v.", err)
	}
	apiProcess := api.New(*staticFileDir, scoopClient, bpdbBackend)
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

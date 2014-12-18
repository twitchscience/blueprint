package main

import (
	"flag"
	"os"
	"os/signal"

	"github.com/twitchscience/blueprint/api"
	"github.com/twitchscience/blueprint/core"
	cachingscoopclient "github.com/twitchscience/blueprint/scoopclient/cachingclient"
)

var (
	scoopUrl        = flag.String("scoopURL", "", "the base url for scoop")
	staticFileDir   = flag.String("staticfiles", "./static", "the location to serve static files from")
	transformConfig = flag.String("transformConfig", "transforms_available.json", "config for available transforms in spade")
)

func main() {
	flag.Parse()
	scoopClient := cachingscoopclient.New(*scoopUrl, *transformConfig)
	apiProcess := api.New(*staticFileDir, scoopClient)
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

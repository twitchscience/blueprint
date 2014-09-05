package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/twitchscience/blueprint/api"
	"github.com/twitchscience/blueprint/core"
	cachingscoopclient "github.com/twitchscience/blueprint/scoopclient/cachingclient"
)

var (
	scoopProto      = flag.String("proto", "http", "the protocol to use when connecting to scoop")
	scoopHostname   = flag.String("hostname", "localhost", "the host to connect to scoop on")
	scoopPort       = flag.Uint64("port", 8080, "the port to connect to scoop on")
	staticFileDir   = flag.String("staticfiles", "./static", "the location to serve static files from")
	transformConfig = flag.String("transformConfig", "transforms_available.json", "config for available transforms in spade")
)

func scoopUrl() string {
	return fmt.Sprintf("%s://%s:%d", *scoopProto, *scoopHostname, *scoopPort)
}

func main() {
	flag.Parse()
	scoopClient := cachingscoopclient.New(scoopUrl(), *transformConfig)
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

package cachingscoopclient

import (
	"log"
	"time"

	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/blueprint/scoopclient"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

const (
	MAX_DURATION = 30 * time.Minute
)

type CachingClient struct {
	rawClient scoopclient.ScoopClient
	cache     []scoop_protocol.Config
	ttl       time.Time
}

func New(urlBase string, transformConfig string) scoopclient.ScoopClient {
	cli := scoopclient.New(urlBase, transformConfig)
	var cfgs []scoop_protocol.Config
	return &CachingClient{
		rawClient: cli,
		cache:     cfgs,
		ttl:       time.Now(),
	}
}

func (c *CachingClient) populateCache() ([]scoop_protocol.Config, error) {
	log.Println("Populating Schema Cache")
	res, err := c.rawClient.FetchAllSchemas()
	if err != nil {
		return nil, err
	}
	c.cache = res
	c.ttl = time.Now().Add(MAX_DURATION)
	return c.cache, nil
}

func (c *CachingClient) FetchAllSchemas() ([]scoop_protocol.Config, error) {
	if c.cache == nil {
		return c.populateCache()
	} else if c.ttl.Before(time.Now()) {
		// serve stale results, then populate cache in the background
		defer func() {
			go c.populateCache()
		}()
	}
	return c.cache, nil
}

func (c *CachingClient) FetchSchema(name string) (*scoop_protocol.Config, error) {
	cfgs, err := c.FetchAllSchemas()
	if err != nil {
		return nil, err
	}
	return scoopclient.ExtractCfgFromList(cfgs, name)
}

func (c *CachingClient) CreateSchema(cfg *scoop_protocol.Config) error {
	defer c.Expire()
	return c.rawClient.CreateSchema(cfg)
}

func (c *CachingClient) UpdateSchema(req *core.ClientUpdateSchemaRequest) error {
	defer c.Expire()
	return c.rawClient.UpdateSchema(req)
}

func (c *CachingClient) PropertyTypes() ([]string, error) {
	return c.rawClient.PropertyTypes()
}

func (c *CachingClient) Expire() {
	c.cache = nil
}

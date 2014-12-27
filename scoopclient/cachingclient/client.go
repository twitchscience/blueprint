package cachingscoopclient

import (
	"log"
	"time"

	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/blueprint/scoopclient"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

const (
	// MaxDuration is the maximum time values will be cached in the client.
	MaxDuration = 30 * time.Minute
)

// CachingClient wraps a normal ScoopClient that caches fetches until a write operation is performed.
type CachingClient struct {
	rawClient scoopclient.ScoopClient
	cache     []scoop_protocol.Config
	ttl       time.Time
}

// New allocates a new CachingClient.
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
	c.ttl = time.Now().Add(MaxDuration)
	return c.cache, nil
}

// FetchAllSchemas returns a potentially cached list of existing schemas.
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

// FetchSchema returns potentially cached information about a specific schema.
func (c *CachingClient) FetchSchema(name string) (*scoop_protocol.Config, error) {
	cfgs, err := c.FetchAllSchemas()
	if err != nil {
		return nil, err
	}
	return scoopclient.ExtractCfgFromList(cfgs, name)
}

// CreateSchema creates a schema and expires the client's cache.
func (c *CachingClient) CreateSchema(cfg *scoop_protocol.Config) error {
	defer c.Expire()
	return c.rawClient.CreateSchema(cfg)
}

// UpdateSchema updates a schema and expires the client's cache.
func (c *CachingClient) UpdateSchema(req *core.ClientUpdateSchemaRequest) error {
	defer c.Expire()
	return c.rawClient.UpdateSchema(req)
}

// PropertyTypes returns the list of available property types (uncached).
func (c *CachingClient) PropertyTypes() ([]string, error) {
	return c.rawClient.PropertyTypes()
}

// Expire the client-side cache.
func (c *CachingClient) Expire() {
	c.cache = nil
}

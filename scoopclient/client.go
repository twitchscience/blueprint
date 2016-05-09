package scoopclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

const (
	// SchemaHostConnectTimeout is how long to wait in order to establish a connection with the scoop server.
	SchemaHostConnectTimeout = 5 * time.Second
	// SchemaHostReadTimeout is how long to read from server connections before timing out.
	SchemaHostReadTimeout = 15 * time.Minute
)

// ScoopClient speaks to scoop and gets schema information.
type ScoopClient interface {
	FetchAllSchemas() ([]scoop_protocol.Config, error)
	FetchSchema(name string) (*scoop_protocol.Config, error)
	PropertyTypes() ([]string, error)
	CreateSchema(*scoop_protocol.Config) error
	UpdateSchema(*core.ClientUpdateSchemaRequest) error
}

type client struct {
	hc              *http.Client
	urlBase         string
	transformConfig string
}

func makeScoopHTTPClient(connTimeout, readTimeout time.Duration) func(n, a string) (net.Conn, error) {
	return func(network, address string) (net.Conn, error) {
		conn, err := net.DialTimeout(network, address, connTimeout)
		if err != nil {
			return nil, err
		}
		err = conn.SetReadDeadline(time.Now().Add(readTimeout))
		return conn, err
	}
}

// New creates a new ScoopClient communicating with a given URL and configured with transforms (i.e. SQL types) available.
func New(urlBase string, transformConfig string) ScoopClient {
	hc := &http.Client{
		Transport: &http.Transport{
			Dial:                makeScoopHTTPClient(SchemaHostConnectTimeout, SchemaHostReadTimeout),
			MaxIdleConnsPerHost: 1,
		},
	}
	return &client{
		hc:              hc,
		urlBase:         urlBase,
		transformConfig: transformConfig,
	}
}

func makeAPIURL(host, uri string) string {
	host = strings.TrimSpace(host)
	uri = strings.TrimSpace(uri)
	return fmt.Sprintf("%s/%s", strings.TrimSuffix(host, "/"), strings.TrimPrefix(uri, "/"))
}

// FetchAllSchemas returns all existing schemas.
func (c *client) FetchAllSchemas() ([]scoop_protocol.Config, error) {
	b, err := c.makeRequest(makeAPIURL(c.urlBase, "/schema/"))
	if err != nil {
		return nil, fmt.Errorf("Error fetching schemas: %s", err.Error())
	}
	var cfgs []scoop_protocol.Config
	err = json.Unmarshal(b, &cfgs)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshalling json: %s, error: %s", string(b), err.Error())
	}
	return cfgs, nil
}

// FetchSchema returns a specific schema.
func (c *client) FetchSchema(name string) (*scoop_protocol.Config, error) {
	cfgs, err := c.FetchAllSchemas()
	if err != nil {
		return nil, err
	}
	return ExtractCfgFromList(cfgs, name)
}

// ExtractCfgFromList finds a specific config from a list of configs.
func ExtractCfgFromList(cfgs []scoop_protocol.Config, name string) (*scoop_protocol.Config, error) {
	for _, cfg := range cfgs {
		if cfg.EventName == name {
			return &cfg, nil
		}
	}
	return nil, fmt.Errorf("Unable to find schema: %s", name)
}

func makeJSONRequest(method, url string, payload []byte) (*http.Request, error) {
	b := bytes.NewBuffer(payload)
	req, err := http.NewRequest(method, url, b)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func (c *client) putJSON(url string, body []byte) (*http.Response, error) {
	req, err := makeJSONRequest("PUT", url, body)
	if err != nil {
		return nil, err
	}
	return c.hc.Do(req)
}

func (c *client) postJSON(url string, body []byte) (*http.Response, error) {
	req, err := makeJSONRequest("POST", url, body)
	if err != nil {
		return nil, err
	}
	return c.hc.Do(req)
}

// CreateSchema creates a new schema.
func (c *client) CreateSchema(cfg *scoop_protocol.Config) error {
	b, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	res, err := c.putJSON(makeAPIURL(c.urlBase, "/schema/"), b)
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		defer func() {
			err = res.Body.Close()
			if err != nil {
				log.Printf("Error closing response body: %v.", err)
			}
		}()
		b, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("Error creating schema: %s, %s", cfg.EventName, b)
	}
	return nil
}

// UpdateSchema updates an existing schema.
func (c *client) UpdateSchema(req *core.ClientUpdateSchemaRequest) error {
	b, err := json.Marshal(req.ConvertToScoopRequest())
	if err != nil {
		return err
	}
	urlPart := fmt.Sprintf("/schema/%s", req.EventName)
	res, err := c.postJSON(makeAPIURL(c.urlBase, urlPart), b)
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		defer func() {
			err = res.Body.Close()
			if err != nil {
				log.Printf("Error closing response body: %v.", err)
			}
		}()
		b, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("Error updating schema: %s, %s", req.EventName, b)
	}
	return nil
}

// PropertyTypes returns the available column types.
func (c *client) PropertyTypes() ([]string, error) {
	f, err := ioutil.ReadFile(c.transformConfig)
	if err != nil {
		return nil, err
	}
	var res sort.StringSlice
	err = json.Unmarshal(f, &res)
	if err != nil {
		return nil, err
	}
	res.Sort()
	return res, nil
}

func (c *client) makeRequest(url string) ([]byte, error) {
	res, err := c.hc.Get(url)
	if err != nil {
		return nil, fmt.Errorf("Error fetching url: %s, Error: %s", url, err.Error())
	}

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("Non-200 status fetching url: %s, StatusCode: %d", url, res.StatusCode)
	}

	defer func() {
		err = res.Body.Close()
		if err != nil {
			log.Printf("Error closing response body: %v.", err)
		}
	}()

	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("Error reading response body: %s", err.Error())
	}
	return b, nil
}

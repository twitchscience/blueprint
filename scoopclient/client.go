package scoopclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

const (
	SCHEMA_HOST_CONNECT_TIMEOUT = 5 * time.Second
	SCHEMA_HOST_READ_TIMEOUT    = 40 * time.Second
)

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
		conn.SetReadDeadline(time.Now().Add(readTimeout))
		return conn, nil
	}
}

func New(urlBase string, transformConfig string) ScoopClient {
	hc := &http.Client{
		Transport: &http.Transport{
			Dial:                makeScoopHTTPClient(SCHEMA_HOST_CONNECT_TIMEOUT, SCHEMA_HOST_READ_TIMEOUT),
			MaxIdleConnsPerHost: 1,
		},
	}
	return &client{
		hc:              hc,
		urlBase:         urlBase,
		transformConfig: transformConfig,
	}
}

func makeApiUrl(host, uri string) string {
	host = strings.TrimSpace(host)
	uri = strings.TrimSpace(uri)
	return fmt.Sprintf("%s/%s", strings.TrimSuffix(host, "/"), strings.TrimPrefix(uri, "/"))
}

func (c *client) FetchAllSchemas() ([]scoop_protocol.Config, error) {
	b, err := c.makeRequest(makeApiUrl(c.urlBase, "/schema/"))
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

func (c *client) FetchSchema(name string) (*scoop_protocol.Config, error) {
	cfgs, err := c.FetchAllSchemas()
	if err != nil {
		return nil, err
	}
	return ExtractCfgFromList(cfgs, name)
}

func ExtractCfgFromList(cfgs []scoop_protocol.Config, name string) (*scoop_protocol.Config, error) {
	for _, cfg := range cfgs {
		if cfg.EventName == name {
			return &cfg, nil
		}
	}
	return nil, fmt.Errorf("Unable to find schema: %s", name)
}

func makeJsonRequest(method, url string, payload []byte) (*http.Request, error) {
	b := bytes.NewBuffer(payload)
	req, err := http.NewRequest(method, url, b)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func (c *client) putJson(url string, body []byte) (*http.Response, error) {
	req, err := makeJsonRequest("PUT", url, body)
	if err != nil {
		return nil, err
	}
	return c.hc.Do(req)
}

func (c *client) postJson(url string, body []byte) (*http.Response, error) {
	req, err := makeJsonRequest("POST", url, body)
	if err != nil {
		return nil, err
	}
	return c.hc.Do(req)
}

func (c *client) CreateSchema(cfg *scoop_protocol.Config) error {
	b, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	res, err := c.putJson(makeApiUrl(c.urlBase, "/schema/"), b)
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		defer res.Body.Close()
		b, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("Error creating schema: %s, %s", cfg.EventName, b)
	}
	return nil
}

func (c *client) UpdateSchema(req *core.ClientUpdateSchemaRequest) error {
	b, err := json.Marshal(req.ConvertToScoopRequest())
	if err != nil {
		return err
	}
	urlPart := fmt.Sprintf("/schema/%s", req.EventName)
	res, err := c.postJson(makeApiUrl(c.urlBase, urlPart), b)
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		defer res.Body.Close()
		b, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("Error updating schema: %s, %s", req.EventName, b)
	}
	return nil
}

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
		return nil, fmt.Errorf("Error fetching url:%s, StatusCode: %d, Error:%s", url, res.StatusCode, err.Error())
	}

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("Non-200 status fetching url: %s, StatusCode: %d", url, res.StatusCode)
	}

	defer res.Body.Close()

	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("Error reading response body: %s", err.Error())
	}
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("Error From scoop:%s, StatusCode: %d, Error:%s", url, res.StatusCode, b)
	}
	return b, nil
}

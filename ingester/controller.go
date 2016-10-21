// Package ingester provides an interface to the ingester's control endpoints.
package ingester

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

// Controller is used to issue requests to the ingester.
type Controller interface {
	ForceIngest(string) error
	TableExists(string) (bool, error)
}

type controller struct {
	ingesterURL string
}

// NewController returns a controller for the ingester at the given URL.
func NewController(ingesterURL string) Controller {
	return &controller{ingesterURL}
}

// ForceIngest causes an ingest of the given table to happen as soon as possible.
func (c *controller) ForceIngest(tableName string) (err error) {
	action := "ForceIngest"
	resp, err := c.sendRequest("/control/ingest", map[string]interface{}{"Table": tableName}, 5*time.Second)
	if err != nil {
		return fmt.Errorf("error making %s request to ingester: %v", action, err)
	}
	defer func() {
		cerr := resp.Body.Close()
		if cerr != nil && err == nil {
			err = fmt.Errorf("failed to close %s response body: %v", action, cerr)
		}
	}()

	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return decodeErrorResponse(resp.Body, resp.StatusCode, action)
}

func (c *controller) sendRequest(path string, params map[string]interface{}, timeout time.Duration) (*http.Response, error) {
	js, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON for ingester request: %v", err)
	}
	req, err := http.NewRequest("POST", c.ingesterURL+path, bytes.NewBuffer(js))
	if err != nil {
		return nil, fmt.Errorf("error building request to ingester: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: timeout}
	return client.Do(req)
}

// TableExists returns whether the given table exists in Ace.
func (c *controller) TableExists(tableName string) (tableExists bool, err error) {
	url := fmt.Sprintf("%s/control/table_exists/%s", c.ingesterURL, tableName)
	req, err := http.NewRequest("GET", url, nil)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("error making TableExists request: %v", err)
	}
	defer func() {
		cerr := resp.Body.Close()
		if cerr != nil && err == nil {
			err = fmt.Errorf("failed to close TableExists response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return false, decodeErrorResponse(resp.Body, resp.StatusCode, "TableExists")
	}
	var exists struct{ Exists bool }
	err = json.NewDecoder(resp.Body).Decode(&exists)
	if err != nil {
		return false, fmt.Errorf("error decoding TableExists response: %v", err)
	}
	return exists.Exists, nil
}

func decodeErrorResponse(respBody io.Reader, statusCode int, action string) error {
	body, err := ioutil.ReadAll(respBody)
	if err != nil {
		return fmt.Errorf("failed to read from %s response: %v", action, err)
	}
	var ingErr struct{ Error string }
	err = json.Unmarshal(body, &ingErr)
	if err != nil {
		return fmt.Errorf("failed to unmarhsal %s response (status code %d, body \"%s\"): %v",
			action, statusCode, body, err)
	}
	return fmt.Errorf("internal ingester error on %s: %s", action, ingErr.Error)
}

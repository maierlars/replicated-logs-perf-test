package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type Context struct {
	Endpoint  url.URL
	Client    *http.Client
	DBServers []string
}

func (c *Context) createReplicatedLog(id uint, config Config) error {
	type Definition struct {
		Id           uint   `json:"id"`
		TargetConfig Config `json:"config"`
	}

	def := Definition{
		Id:           id,
		TargetConfig: config,
	}

	body, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("Error while creating log: %w", err)
	}

	url := c.Endpoint
	url.Path = "_api/log"
	resp, err := c.Client.Post(url.String(), "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("Error while creating log: %w", err)
	}
	var target struct {
		Code         int    `json:"code,omitempty"`
		Error        bool   `json:"error,omitempty"`
		ErrorMessage string `json:"errorMessage,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&target); err != nil {
		return fmt.Errorf("Error while reading the response: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 || target.Error {
		return fmt.Errorf("Error while creating log: status-code=%d, error-code=%d, message=%s", resp.StatusCode, target.Code, target.ErrorMessage)
	}

	return nil
}

func (c *Context) dropReplicatedLog(id uint) error {
	url := c.Endpoint
	url.Path = fmt.Sprintf("_api/log/%d", id)
	req, err := http.NewRequest("DELETE", url.String(), nil)
	if err != nil {
		return fmt.Errorf("Error while dropping log: %w", err)
	}

	resp, err := c.Client.Do(req)
	if err != nil {
		return fmt.Errorf("Error while dropping log: %w", err)
	}
	if _, err = io.Copy(ioutil.Discard, resp.Body); err != nil {
		return fmt.Errorf("Failed to read body: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 202 {
		return fmt.Errorf("Error while dropping log: Unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

func (c *Context) waitForReplicatedLog(id uint) error {
	for {
		url := c.Endpoint
		url.Path = fmt.Sprintf("_api/log/%d", id)
		resp, err := c.Client.Get(url.String())
		if err != nil {
			return fmt.Errorf("Error while requesting log status: %w", err)
		}

		var target struct {
			Code   int  `json:"code,omitempty"`
			Error  bool `json:"error,omitempty"`
			Result struct {
				LeaderId string `json:"leaderId,omitempty"`
			} `json:"result,omitempty"`
		}

		json.NewDecoder(resp.Body).Decode(&target)
		defer resp.Body.Close()
		if resp.StatusCode == 200 && len(target.Result.LeaderId) > 0 {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (c *Context) printStatus(id int) error {
	url := c.Endpoint
	url.Path = fmt.Sprintf("_api/log/%d", id)
	resp, err := c.Client.Get(url.String())
	if err != nil {
		return err
	}
	if _, err = io.Copy(ioutil.Discard, resp.Body); err != nil {
		return fmt.Errorf("Failed to read body: %w", err)
	}
	defer resp.Body.Close()

	fmt.Printf("Status: %s\n", resp.Body)
	return nil
}

func (c *Context) insertReplicatedLog(id uint, payload interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("Error while inserting entry: %w", err)
	}
	url := c.Endpoint
	url.Path = fmt.Sprintf("_api/log/%d/insert", id)
	resp, err := c.Client.Post(url.String(), "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("Error while inserting entry: %w", err)
	}
	if _, err = io.Copy(ioutil.Discard, resp.Body); err != nil {
		return fmt.Errorf("Failed to read body: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		return fmt.Errorf("Error while inserting entry: Unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

func (c *Context) getDatabaseServers() (error, []string) {
	if c.DBServers != nil {
		return nil, c.DBServers
	}

	url := c.Endpoint
	url.Path = "_admin/cluster/health"
	resp, err := c.Client.Get(url.String())
	if err != nil {
		return fmt.Errorf("Error while requesting database servers: %w", err), nil
	}

	var health struct {
		Code   int                    `json:"code,omitempty"`
		Error  bool                   `json:"error,omitempty"`
		Health map[string]interface{} `json:"Health,omitempty"`
	}

	json.NewDecoder(resp.Body).Decode(&health)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("Error while requesting database servers: unexpected status code %d", resp.StatusCode), nil
	}

	var result []string
	for key := range health.Health {
		if strings.HasPrefix(key, "PRMR-") {
			result = append(result, key)
		}
	}

	c.DBServers = result
	fmt.Fprintf(os.Stderr, "Using Database Server: %v\n", result)
	return nil, result
}

func (c *Context) createReplicatedState(id uint, config Config, implementation string) error {
	type Implementation struct {
		Type string `json:"type"`
	}
	type Properties struct {
		Implementation Implementation `json:"implementation"`
	}
	type Definition struct {
		Id           uint                `json:"id"`
		Config       Config              `json:"config"`
		Properties   Properties          `json:"properties"`
		Participants map[string]struct{} `json:"participants"`
	}

	err, dbservers := c.getDatabaseServers()
	if err != nil {
		return err
	}

	def := Definition{
		Id:     id,
		Config: config,
		Properties: Properties{
			Implementation: Implementation{
				Type: implementation,
			},
		},
		Participants: make(map[string]struct{}),
	}

	for i := 0; i < int(config.ReplicationFactor); i++ {
		def.Participants[dbservers[i]] = struct{}{}
	}

	body, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("Error while creating log: %w", err)
	}

	url := c.Endpoint
	url.Path = "_api/replicated-state"
	resp, err := c.Client.Post(url.String(), "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("Error while creating replicated state: %w", err)
	}
	var target struct {
		Code         int    `json:"code,omitempty"`
		Error        bool   `json:"error,omitempty"`
		ErrorMessage string `json:"errorMessage,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&target); err != nil {
		return fmt.Errorf("Error while reading the response: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 || target.Error {
		return fmt.Errorf("Error while creating replicated state: status-code=%d, error-code=%d, message=%s", resp.StatusCode, target.Code, target.ErrorMessage)
	}

	return nil
}

func (c *Context) prototypeStateSetKey(id uint, key string, value string) error {
	payload := make(map[string]string)
	payload[key] = value
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("Error while inserting entry: %w", err)
	}
	url := c.Endpoint
	url.Path = fmt.Sprintf("_api/prototype-state/%d/insert", id)
	resp, err := c.Client.Post(url.String(), "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("Error while inserting entry: %w", err)
	}
	if _, err = io.Copy(ioutil.Discard, resp.Body); err != nil {
		return fmt.Errorf("Failed to read body: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("Error while inserting entry: Unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

func (c *Context) waitForPrototypeState(id uint) error {
	for {
		if err := c.prototypeStateSetKey(id, "_test", "value"); err == nil {
			return nil
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func (c *Context) checkPrototypeStateAvailable() error {
	url := c.Endpoint
	url.Path = "_api/prototype-state"
	resp, err := c.Client.Post(url.String(), "", nil)
	if err != nil {
		return err
	}
	if resp.StatusCode != 400 {
		return fmt.Errorf("prototype api not available")
	}
	return nil
}

func NewContext(endpoint *url.URL) *Context {
	return &Context{
		Client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 1000,
			},
			Timeout: 10 * time.Second,
		},
		Endpoint: *endpoint,
	}
}

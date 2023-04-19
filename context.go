package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

type Context struct {
	Endpoint url.URL
	Client   *http.Client
}

type DatabaseContext struct {
	Context
	Database string
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
		return fmt.Errorf("error while creating log: %w", err)
	}

	url := c.Endpoint
	url.Path = "_api/log"
	resp, err := c.Client.Post(url.String(), "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("error while creating log: %w", err)
	}
	var target struct {
		Code         int    `json:"code,omitempty"`
		Error        bool   `json:"error,omitempty"`
		ErrorMessage string `json:"errorMessage,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&target); err != nil {
		return fmt.Errorf("error while reading the response: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 || target.Error {
		return fmt.Errorf("error while creating log: status-code=%d, error-code=%d, message=%s", resp.StatusCode, target.Code, target.ErrorMessage)
	}

	return nil
}

func (c *Context) dropReplicatedLog(id uint) error {
	url := c.Endpoint
	url.Path = fmt.Sprintf("_api/log/%d", id)
	req, err := http.NewRequest("DELETE", url.String(), nil)
	if err != nil {
		return fmt.Errorf("error while dropping log: %w", err)
	}

	resp, err := c.Client.Do(req)
	if err != nil {
		return fmt.Errorf("error while dropping log: %w", err)
	}
	if _, err = io.Copy(ioutil.Discard, resp.Body); err != nil {
		return fmt.Errorf("failed to read body: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("error while dropping log: Unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

func (c *Context) waitForReplicatedLog(id uint) error {
	for {
		url := c.Endpoint
		url.Path = fmt.Sprintf("_api/log/%d", id)
		resp, err := c.Client.Get(url.String())
		if err != nil {
			return fmt.Errorf("error while requesting log status: %w", err)
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
		return fmt.Errorf("failed to read body: %w", err)
	}
	defer resp.Body.Close()

	fmt.Printf("Status: %s\n", resp.Body)
	return nil
}

func (c *Context) insertReplicatedLog(id uint, payload interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error while inserting entry: %w", err)
	}
	url := c.Endpoint
	url.Path = fmt.Sprintf("_api/log/%d/insert", id)
	resp, err := c.Client.Post(url.String(), "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("error while inserting entry: %w", err)
	}
	if _, err = io.Copy(ioutil.Discard, resp.Body); err != nil {
		return fmt.Errorf("failed to read body: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 && resp.StatusCode != 202 {
		return fmt.Errorf("error while inserting entry: Unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

type DatabaseOptions struct {
	ReplicationVersion *string `json:"replicationVersion,omitempty"`
}

func (c *Context) createDatabase(name string, opts *DatabaseOptions) error {

	type CreateDatabaseBody struct {
		Name    string           `json:"name"`
		Options *DatabaseOptions `json:"options,omitempty"`
	}

	req := CreateDatabaseBody{name, opts}
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("error while creating database: %w", err)
	}
	url := c.Endpoint
	url.Path = "_api/database"
	resp, err := c.Client.Post(url.String(), "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("error while creating database: %w", err)
	}
	if _, err = io.Copy(ioutil.Discard, resp.Body); err != nil {
		return fmt.Errorf("failed to read body: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		return fmt.Errorf("error while creating database: %d", resp.StatusCode)
	}
	return nil
}

func (c *Context) dropDatabase(name string) error {

	url := c.Endpoint
	url.Path = fmt.Sprintf("_api/database/%s", name)
	req, err := http.NewRequest("DELETE", url.String(), nil)
	if err != nil {
		return fmt.Errorf("error while dropping database: %w", err)
	}

	resp, err := c.Client.Do(req)
	if err != nil {
		return fmt.Errorf("error while dropping database: %w", err)
	}
	if _, err = io.Copy(ioutil.Discard, resp.Body); err != nil {
		return fmt.Errorf("failed to read body: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("error while dropping database: unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

func (c *Context) openDatabase(dbname string) *DatabaseContext {
	return &DatabaseContext{
		Context: *c, Database: dbname,
	}
}

type CreateCollectionOptions struct {
	Name              string `json:"name"`
	WriteConcern      uint   `json:"writeConcern"`
	ReplicationFactor uint   `json:"replicationFactor"`
	NumberOfShards    uint   `json:"numberOfShards"`
	WaitForSync       bool   `json:"waitForSync"`
}

func (c *DatabaseContext) createCollection(opts CreateCollectionOptions) error {

	body, err := json.Marshal(opts)
	if err != nil {
		return fmt.Errorf("error while creating collection: %w", err)
	}
	url := c.Endpoint
	url.Path = fmt.Sprintf("/_db/%s/_api/collection", c.Database)
	resp, err := c.Client.Post(url.String(), "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("error while creating collection: %w", err)
	}
	if _, err = io.Copy(ioutil.Discard, resp.Body); err != nil {
		return fmt.Errorf("failed to read body: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("error while creating collection: %d", resp.StatusCode)
	}
	return nil
}

func (c *DatabaseContext) insertDocument(collection string, doc interface{}) error {

	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("error while creating collection: %w", err)
	}
	url := c.Endpoint
	url.Path = fmt.Sprintf("/_db/%s/_api/document/c", c.Database)
	resp, err := c.Client.Post(url.String(), "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("error while creating document: %w", err)
	}
	if _, err = io.Copy(ioutil.Discard, resp.Body); err != nil {
		return fmt.Errorf("failed to read body: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 && resp.StatusCode != 202 {
		return fmt.Errorf("error while creating document: %d", resp.StatusCode)
	}
	return nil
}

func NewContext(endpoint *url.URL) *Context {
	return &Context{
		Client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 1000,
			},
			Timeout: 30 * time.Second,
		},
		Endpoint: *endpoint,
	}
}

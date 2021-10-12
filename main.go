package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"sync"
	"time"
)

type Context struct {
	Endpoint url.URL
	Client   *http.Client

	nextLogId int
}

type Config struct {
	ReplicationFactor uint `json:"replicationFactor"`
	WriteConcern      uint `json:"writeConcern"`
	WaitForSync       bool `json:"waitForSync"`
}

type Definition struct {
	Id           uint   `json:"id"`
	TargetConfig Config `json:"targetConfig"`
}

func (c *Context) createReplicatedLog(id uint, config Config) error {
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
	if _, err = io.ReadAll(resp.Body); err != nil {
		return fmt.Errorf("Failed to read body: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("Error while creating log: Unexcepted status code: %d", resp.StatusCode)
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
	if _, err = io.ReadAll(resp.Body); err != nil {
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
		if _, err = io.ReadAll(resp.Body); err != nil {
			return fmt.Errorf("Failed to read body: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == 200 {
			resp.Body.Close()
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
	if _, err = io.ReadAll(resp.Body); err != nil {
		return fmt.Errorf("Failed to read body: %w", err)
	}
	defer resp.Body.Close()

	fmt.Printf("Status: %s\n", resp.Body)
	return nil
}

func (c *Context) insert(id uint, payload interface{}) error {
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
	if _, err = io.ReadAll(resp.Body); err != nil {
		return fmt.Errorf("Failed to read body: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 202 {
		return fmt.Errorf("Error while inserting entry: Unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

type TestCase struct {
	NumberOfRequests int    `json:"numberOfRequests"`
	NumberOfThreads  int    `json:"numberOfThreads"`
	Config           Config `json:"config"`
}

type LogEntry struct {
	Client int `json:"client"`
	Index  int `json:"index"`
}

type TestResult struct {
	Min               float64 `json:"min"`
	Max               float64 `json:"max"`
	Percent99         float64 `json:"p99"`
	Percent99p9       float64 `json:"p99.9"`
	Avg               float64 `json:"avg"`
	RequsterPerSecond float64 `json:"rps"`
	Total             float64 `json:"total"`
}

type ResultEntry struct {
	Name   string     `json:"name"`
	Test   TestCase   `json:"test"`
	Result TestResult `json:"result"`
}

func calcResults(total time.Duration, requests []time.Duration) TestResult {
	sort.Slice(requests, func(a, b int) bool {
		return int64(requests[a]) < int64(requests[b])
	})

	nr := len(requests)

	sum := time.Duration(0)
	for _, time := range requests {
		sum += time
	}

	return TestResult{
		Min:               float64(requests[0].Seconds()),
		Max:               float64(requests[nr-1].Seconds()),
		Avg:               float64((sum / time.Duration(nr)).Seconds()),
		RequsterPerSecond: float64(nr) / total.Seconds(),
		Total:             total.Seconds(),
	}
}

func (c *Context) runTest(id uint, test TestCase) (*TestResult, error) {
	if err := c.createReplicatedLog(id, test.Config); err != nil {
		return nil, err
	}
	defer func() {
		if err := c.dropReplicatedLog(id); err != nil {
			fmt.Printf("Dropping of replicated log %d failed: %v", id, err)
		}
	}()
	if err := c.waitForReplicatedLog(id); err != nil {
		return nil, err
	}

	results := make([]time.Duration, test.NumberOfRequests*test.NumberOfThreads)

	wg := sync.WaitGroup{}

	start := time.Now()
	for i := 0; i < test.NumberOfThreads; i++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			for k := 0; k < test.NumberOfRequests; k++ {
				entry := LogEntry{j, k}
				req_start := time.Now()
				if err := c.insert(id, entry); err != nil {
					fmt.Printf("Request failed: %v", err)
				}
				results[j*test.NumberOfRequests+k] = time.Since(req_start)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)
	calc := calcResults(duration, results)
	return &calc, nil
}

func testName(test TestCase) string {
	name := fmt.Sprintf("insert-c%d-r%d-wc%d", test.NumberOfThreads, test.Config.ReplicationFactor, test.Config.WriteConcern)
	if test.Config.WaitForSync {
		name = name + "-ws"
	}
	return name
}

var testCases = []TestCase{
	{
		NumberOfRequests: 1000,
		NumberOfThreads:  1,
		Config: Config{
			WriteConcern:      2,
			ReplicationFactor: 3,
			WaitForSync:       true,
		},
	},
	{
		NumberOfRequests: 1000,
		NumberOfThreads:  1,
		Config: Config{
			WriteConcern:      1,
			ReplicationFactor: 3,
			WaitForSync:       true,
		},
	},
	{
		NumberOfRequests: 1000,
		NumberOfThreads:  10,
		Config: Config{
			WriteConcern:      2,
			ReplicationFactor: 3,
			WaitForSync:       true,
		},
	},
	{
		NumberOfRequests: 1000,
		NumberOfThreads:  100,
		Config: Config{
			WriteConcern:      2,
			ReplicationFactor: 3,
			WaitForSync:       true,
		},
	},
	{
		NumberOfRequests: 10000,
		NumberOfThreads:  1,
		Config: Config{
			WriteConcern:      2,
			ReplicationFactor: 3,
			WaitForSync:       false,
		},
	},
	{
		NumberOfRequests: 10000,
		NumberOfThreads:  10,
		Config: Config{
			WriteConcern:      2,
			ReplicationFactor: 3,
			WaitForSync:       false,
		},
	},
	{
		NumberOfRequests: 1000,
		NumberOfThreads:  100,
		Config: Config{
			WriteConcern:      2,
			ReplicationFactor: 3,
			WaitForSync:       false,
		},
	},
	{
		NumberOfRequests: 1000,
		NumberOfThreads:  1,
		Config: Config{
			WriteConcern:      1,
			ReplicationFactor: 3,
			WaitForSync:       false,
		},
	},
}

type Arguments struct {
	Endpoint string
	OutFile  *os.File
}

func runAllTests(args Arguments) error {
	endpoint, err := url.Parse(args.Endpoint)
	if err != nil {
		return fmt.Errorf("Failed to parse endpoitn: %w", err)
	}

	ctx := Context{
		Client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 1000,
			},
		},
		Endpoint: *endpoint,
	}

	for idx, test := range testCases {
		result, err := ctx.runTest(550+uint(idx), test)
		if err != nil {
			panic(err)
		}
		out, _ := json.Marshal(ResultEntry{
			Name:   testName(test),
			Test:   test,
			Result: *result,
		})
		fmt.Fprintf(args.OutFile, "%s\n", out)
	}

	return nil
}

func parseArguments() (*Arguments, error) {
	outFileName := flag.String("out-file", "-", "specifies the output file, '-' is stdout.")
	flag.Parse()
	args := flag.Args()
	if len(args) != 1 {
		return nil, fmt.Errorf("Expected a single position argument, found %d", len(args))
	}

	outFile, err := func() (*os.File, error) {
		if *outFileName != "-" {
			return os.Create(*outFileName)
		}

		return os.Stdout, nil
	}()
	if err != nil {
		return nil, fmt.Errorf("Failed to open output file: %w", err)
	}

	return &Arguments{Endpoint: args[0], OutFile: outFile}, nil
}

func main() {
	args, err := parseArguments()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse arguments: %v\n", err)
		return
	}

	if err := runAllTests(*args); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to run all tests: %v\n", err)
	}
}

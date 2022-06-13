package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"
)

type Config struct {
	ReplicationFactor uint `json:"replicationFactor"`
	WriteConcern      uint `json:"writeConcern"`
	SoftWriteConcern  uint `json:"softWriteConcern"`
	WaitForSync       bool `json:"waitForSync"`
}

const NumberOfTestRuns = uint(1)

type ResultEntry struct {
	Name    string                       `json:"name"`
	Test    TestSettings                 `json:"test"`
	Result  TestResult                   `json:"result"`
	Details [NumberOfTestRuns]TestResult `json:"details"`
}

func (c *Context) runTestImpl(id uint, test *TestCase) (*TestResult, error) {
	if err := test.Implementation.SetupTest(c, id, test.Settings); err != nil {
		return nil, err
	}
	defer func() {
		if err := test.Implementation.TearDownTest(c, id); err != nil {
			fmt.Fprintf(os.Stderr, "Tear down of test %s (%d) failed: %v\n", test.Implementation.GetTestName(test.Settings), id, err)
		}
	}()

	results := make([]time.Duration, test.Settings.NumberOfRequests*test.Settings.NumberOfThreads)

	wg := sync.WaitGroup{}
	errch := make(chan error, test.Settings.NumberOfThreads)

	start := time.Now()
	for i := 0; i < test.Settings.NumberOfThreads; i++ {
		wg.Add(1)
		slice := results[i*test.Settings.NumberOfRequests : (i+1)*test.Settings.NumberOfRequests]
		go func(i int) {
			defer wg.Done()
			err := test.Implementation.RunTestThread(c, id, test.Settings, i, slice)
			if err != nil {
				errch <- err
			}
		}(i)
	}

	wg.Wait()
	select {
	case err, ok := <-errch:
		if ok {
			return nil, err
		}
		break
	default:
		break
	}

	duration := time.Since(start)
	calc := calcResults(duration, results)
	return &calc, nil
}

func testName(test *TestCase) string {
	return test.Implementation.GetTestName(test.Settings)
}

type TestCase struct {
	Settings       TestSettings
	Implementation TestImplementation
}

var testCases = []TestCase{
	{
		Settings: TestSettings{
			NumberOfRequests: 10000,
			NumberOfThreads:  1,
			Config: Config{
				WriteConcern:      2,
				SoftWriteConcern:  2,
				ReplicationFactor: 3,
				WaitForSync:       true,
			},
		},
		Implementation: &ReplicatedLogsTest{},
	},
	{
		Settings: TestSettings{
			NumberOfRequests: 10000,
			NumberOfThreads:  1,
			Config: Config{
				WriteConcern:      1,
				SoftWriteConcern:  1,
				ReplicationFactor: 3,
				WaitForSync:       true,
			},
		},
		Implementation: &ReplicatedLogsTest{},
	},
	{
		Settings: TestSettings{
			NumberOfRequests: 10000,
			NumberOfThreads:  10,
			Config: Config{
				WriteConcern:      2,
				SoftWriteConcern:  2,
				ReplicationFactor: 3,
				WaitForSync:       true,
			},
		},
		Implementation: &ReplicatedLogsTest{},
	},
	{
		Settings: TestSettings{
			NumberOfRequests: 10000,
			NumberOfThreads:  100,
			Config: Config{
				WriteConcern:      2,
				SoftWriteConcern:  2,
				ReplicationFactor: 3,
				WaitForSync:       true,
			},
		},
		Implementation: &ReplicatedLogsTest{},
	},
	{
		Settings: TestSettings{
			NumberOfRequests: 100000,
			NumberOfThreads:  1,
			Config: Config{
				WriteConcern:      2,
				SoftWriteConcern:  2,
				ReplicationFactor: 3,
				WaitForSync:       false,
			},
		},
		Implementation: &ReplicatedLogsTest{},
	},
	{
		Settings: TestSettings{
			NumberOfRequests: 100000,
			NumberOfThreads:  10,
			Config: Config{
				WriteConcern:      2,
				SoftWriteConcern:  2,
				ReplicationFactor: 3,
				WaitForSync:       false,
			},
		},
		Implementation: &ReplicatedLogsTest{},
	},
	{
		Settings: TestSettings{
			NumberOfRequests: 10000,
			NumberOfThreads:  100,
			Config: Config{
				WriteConcern:      2,
				SoftWriteConcern:  2,
				ReplicationFactor: 3,
				WaitForSync:       false,
			},
		},
		Implementation: &ReplicatedLogsTest{},
	},
	{
		Settings: TestSettings{
			NumberOfRequests: 10000,
			NumberOfThreads:  1,
			Config: Config{
				WriteConcern:      1,
				SoftWriteConcern:  1,
				ReplicationFactor: 3,
				WaitForSync:       false,
			},
		},
		Implementation: &ReplicatedLogsTest{},
	},
	// Replicated State Tests
	{
		Settings: TestSettings{
			NumberOfRequests: 10000,
			NumberOfThreads:  1,
			Config: Config{
				WriteConcern:      1,
				SoftWriteConcern:  1,
				ReplicationFactor: 3,
				WaitForSync:       false,
			},
		},
		Implementation: &PrototypeStateTests{},
	},
	{
		Settings: TestSettings{
			NumberOfRequests: 10000,
			NumberOfThreads:  10,
			Config: Config{
				WriteConcern:      1,
				SoftWriteConcern:  1,
				ReplicationFactor: 3,
				WaitForSync:       false,
			},
		},
		Implementation: &PrototypeStateTests{},
	},
	{
		Settings: TestSettings{
			NumberOfRequests: 10000,
			NumberOfThreads:  100,
			Config: Config{
				WriteConcern:      1,
				SoftWriteConcern:  1,
				ReplicationFactor: 3,
				WaitForSync:       false,
			},
		},
		Implementation: &PrototypeStateTests{},
	},
	{
		Settings: TestSettings{
			NumberOfRequests: 10000,
			NumberOfThreads:  1,
			Config: Config{
				WriteConcern:      2,
				SoftWriteConcern:  2,
				ReplicationFactor: 3,
				WaitForSync:       false,
			},
		},
		Implementation: &PrototypeStateTests{},
	},
	{
		Settings: TestSettings{
			NumberOfRequests: 10000,
			NumberOfThreads:  10,
			Config: Config{
				WriteConcern:      2,
				SoftWriteConcern:  2,
				ReplicationFactor: 3,
				WaitForSync:       false,
			},
		},
		Implementation: &PrototypeStateTests{},
	},
	{
		Settings: TestSettings{
			NumberOfRequests: 10000,
			NumberOfThreads:  100,
			Config: Config{
				WriteConcern:      2,
				SoftWriteConcern:  2,
				ReplicationFactor: 3,
				WaitForSync:       false,
			},
		},
		Implementation: &PrototypeStateTests{},
	},
	{
		Settings: TestSettings{
			NumberOfRequests: 10000,
			NumberOfThreads:  1,
			Config: Config{
				WriteConcern:      2,
				SoftWriteConcern:  2,
				ReplicationFactor: 3,
				WaitForSync:       true,
			},
		},
		Implementation: &PrototypeStateTests{},
	},
	{
		Settings: TestSettings{
			NumberOfRequests: 10000,
			NumberOfThreads:  10,
			Config: Config{
				WriteConcern:      2,
				SoftWriteConcern:  2,
				ReplicationFactor: 3,
				WaitForSync:       true,
			},
		},
		Implementation: &PrototypeStateTests{},
	},
	{
		Settings: TestSettings{
			NumberOfRequests: 10000,
			NumberOfThreads:  100,
			Config: Config{
				WriteConcern:      2,
				SoftWriteConcern:  2,
				ReplicationFactor: 3,
				WaitForSync:       true,
			},
		},
		Implementation: &PrototypeStateTests{},
	},
	{
		Settings: TestSettings{
			NumberOfRequests: 10000,
			NumberOfThreads:  100,
			Config: Config{
				WriteConcern:      3,
				SoftWriteConcern:  3,
				ReplicationFactor: 3,
				WaitForSync:       true,
			},
		},
		Implementation: &PrototypeStateTests{},
	},
}

type Arguments struct {
	Endpoint   string
	OutFile    *os.File
	QuickTests bool
	CustomTest *TestCase
}

func runTestCase(args Arguments, idx int, test *TestCase, ctx *Context) error {

	actualNumberOfRuns := NumberOfTestRuns

	if args.QuickTests {
		test.Settings.NumberOfRequests /= 100
		actualNumberOfRuns = 1
	}

	var results [NumberOfTestRuns]TestResult
	for run := uint(0); run < actualNumberOfRuns; run++ {
		res, err := ctx.runTestImpl(550+uint(idx)*NumberOfTestRuns+run, test)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Test %s, run %d, failed: %v\n", test.Implementation.GetTestName(test.Settings), run, err)
			return err
		}
		results[run] = *res
	}
	result := collectMedians(results[:actualNumberOfRuns])
	out, _ := json.Marshal(ResultEntry{
		Name:    testName(test),
		Test:    test.Settings,
		Details: results,
		Result:  result,
	})
	fmt.Fprintf(args.OutFile, "%s\n", out)
	return nil
}

func runAllTests(args Arguments) error {
	endpoint, err := url.Parse(args.Endpoint)
	if err != nil {
		return fmt.Errorf("Failed to parse endpoitn: %w", err)
	}

	ctx := NewContext(endpoint)
	numErrors := 0
	if args.CustomTest != nil {
		err = runTestCase(args, 0, args.CustomTest, ctx)
		if err != nil {
			numErrors += 1
		}
	} else {
		for idx, test := range testCases {
			err = runTestCase(args, idx, &test, ctx)
			if err != nil {
				numErrors += 1
			}
		}
	}

	if numErrors > 0 {
		return fmt.Errorf("At least one test produced an error")
	}
	return nil
}

func parseArguments() (*Arguments, error) {
	outFileName := flag.String("out-file", "-", "specifies the output file, '-' is stdout.")
	quickTests := flag.Bool("quick", false, "Run quick tests")
	customSettings := flag.String("custom", "", "Specify a json object for a custom test case")
	customImplName := flag.String("impl", "proto", "Select test implementation for custom test. Either `proto` or `log`")
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

	var customTest *TestCase = nil
	if *customSettings != "" {
		var settings TestSettings
		var impl TestImplementation
		if err := json.Unmarshal([]byte(*customSettings), &settings); err != nil {
			return nil, fmt.Errorf("Failed to load test settings: %v", err)
		}
		if *customImplName == "proto" {
			impl = &PrototypeStateTests{}
		} else if *customImplName == "log" {
			impl = &ReplicatedLogsTest{}
		} else {
			return nil, fmt.Errorf("Unknown implementation %s", *customImplName)
		}
		customTest = &TestCase{
			Settings:       settings,
			Implementation: impl,
		}
	}

	return &Arguments{Endpoint: args[0], OutFile: outFile, QuickTests: *quickTests, CustomTest: customTest}, nil
}

func main() {
	args, err := parseArguments()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse arguments: %v\n", err)
		os.Exit(1)
	}

	if err := runAllTests(*args); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to run all tests: %v\n", err)
		os.Exit(1)
	}
}

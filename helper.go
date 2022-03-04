package main

import (
	"reflect"
	"sort"
	"time"
)

type TestSettings struct {
	NumberOfRequests int    `json:"numberOfRequests"`
	NumberOfThreads  int    `json:"numberOfThreads"`
	Config           Config `json:"config"`
}

type TestResult struct {
	Min               float64 `json:"min"`
	Max               float64 `json:"max"`
	Percent99         float64 `json:"p99"`
	Percent99p9       float64 `json:"p99.9"`
	Avg               float64 `json:"avg"`
	RequsterPerSecond float64 `json:"rps"`
	Total             float64 `json:"total"`
	Median            float64 `json:"med"`
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
		Percent99:         requests[int(float64(nr)*0.99)].Seconds(),
		Percent99p9:       requests[int(float64(nr)*0.999)].Seconds(),
		Median:            requests[int(float64(nr)*0.5)].Seconds(),
		Avg:               float64((sum / time.Duration(nr)).Seconds()),
		RequsterPerSecond: float64(nr) / total.Seconds(),
		Total:             total.Seconds(),
	}
}

func collectMedians(results []TestResult) TestResult {
	var result TestResult
	l := len(results)
	t := reflect.TypeOf(result)
	for i := 0; i < t.NumField(); i++ {
		values := make([]float64, l)
		for k := 0; k < l; k++ {
			values[k] = reflect.ValueOf(results[k]).Field(i).Float()
		}

		sort.Slice(values, func(a, b int) bool {
			return values[a] < values[b]
		})

		median := values[len(values)/2]

		reflect.ValueOf(&result).Elem().Field(i).SetFloat(median)
	}

	return result
}

type TestImplementation interface {
	GetTestName(test TestSettings) string
	SetupTest(ctx *Context, id uint, test TestSettings) error
	TearDownTest(ctx *Context, id uint) error
	RunTestThread(ctx *Context, id uint, test TestSettings, threadNo int, results []time.Duration) error
}

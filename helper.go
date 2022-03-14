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
	Min float64 `json:"min"`
	Max float64 `json:"max"`
	Avg float64 `json:"avg"`

	Percent10   float64 `json:"p10"`
	Percent20   float64 `json:"p20"`
	Percent30   float64 `json:"p30"`
	Percent40   float64 `json:"p40"`
	Median      float64 `json:"med"`
	Percent60   float64 `json:"p60"`
	Percent70   float64 `json:"p70"`
	Percent80   float64 `json:"p80"`
	Percent90   float64 `json:"p90"`
	Percent99   float64 `json:"p99"`
	Percent99p9 float64 `json:"p99.9"`

	RequsterPerSecond float64 `json:"rps"`
	Total             float64 `json:"total"`
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

	min := float64(requests[0].Seconds())
	max := float64(requests[nr-1].Seconds())

	return TestResult{
		Min:               min,
		Max:               max,
		Percent10:         requests[int(float64(nr)*0.1)].Seconds(),
		Percent20:         requests[int(float64(nr)*0.2)].Seconds(),
		Percent30:         requests[int(float64(nr)*0.3)].Seconds(),
		Percent40:         requests[int(float64(nr)*0.4)].Seconds(),
		Percent60:         requests[int(float64(nr)*0.6)].Seconds(),
		Percent70:         requests[int(float64(nr)*0.7)].Seconds(),
		Percent80:         requests[int(float64(nr)*0.8)].Seconds(),
		Percent90:         requests[int(float64(nr)*0.9)].Seconds(),
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

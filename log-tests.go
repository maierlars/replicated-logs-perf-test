package main

import (
	"fmt"
	"time"
)

type ReplicatedLogsTest struct{}

type LogEntry struct {
	Client int `json:"client"`
	Index  int `json:"index"`
}

func (ReplicatedLogsTest) RunTestThread(ctx *Context, id uint, test TestSettings, threadNo int, results []time.Duration) error {
	for k := 0; k < test.NumberOfRequests; k++ {
		entry := LogEntry{threadNo, k}
		req_start := time.Now()
		if err := ctx.insertReplicatedLog(id, entry); err != nil {
			return fmt.Errorf("failed to insert log entry during test: %v", err)
		}
		results[k] = time.Since(req_start)
	}

	return nil
}

func (ReplicatedLogsTest) GetTestName(test TestSettings) string {
	name := fmt.Sprintf("insert-c%d-r%d-wc%d", test.NumberOfThreads, test.NumberOfRequests, test.Config.WriteConcern)
	if test.Config.WaitForSync {
		name = name + "-ws"
	}
	return name
}

func (ReplicatedLogsTest) SetupTest(ctx *Context, id uint, test TestSettings) error {
	if err := ctx.createReplicatedLog(id, test.Config); err != nil {
		return err
	}

	if err := ctx.waitForReplicatedLog(id); err != nil {
		ctx.dropReplicatedLog(id)
		return err
	}

	return nil
}

func (ReplicatedLogsTest) TearDownTest(ctx *Context, id uint) error {
	return ctx.dropReplicatedLog(id)
}

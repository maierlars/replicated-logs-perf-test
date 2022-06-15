package main

import (
	"fmt"
	"time"
)

type PrototypeStateTests struct{}

func (PrototypeStateTests) RunTestThread(ctx *Context, id uint, test TestSettings, threadNo int, results []time.Duration) error {
	for k := 0; k < test.NumberOfRequests; k++ {
		req_start := time.Now()
		key := fmt.Sprintf("key-%d-%d", threadNo, k)
		if err := ctx.prototypeStateSetKey(id, key, "value"); err != nil {
			return fmt.Errorf("failed to insert log entry during test: %v", err)
		}
		results[k] = time.Since(req_start)
	}

	return nil
}

func (PrototypeStateTests) GetTestName(test TestSettings) string {
	name := fmt.Sprintf("proto-insert-c%d-r%d-wc%d", test.NumberOfThreads, test.NumberOfServers, test.Config.WriteConcern)
	if test.Config.WaitForSync {
		name = name + "-ws"
	}
	return name
}

func (PrototypeStateTests) SetupTest(ctx *Context, id uint, test TestSettings) error {
	if err := ctx.checkPrototypeStateAvailable(); err != nil {
		return err
	}

	if err := ctx.createReplicatedState(id, test.Config, "prototype", test.NumberOfServers); err != nil {
		return err
	}

	return ctx.waitForPrototypeState(id)
}

func (PrototypeStateTests) TearDownTest(ctx *Context, id uint) error {
	return nil
}

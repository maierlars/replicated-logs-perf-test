package main

import (
	"fmt"
	"time"
)

type DocumentTests struct {
	dbname string
}

type MyDocument struct {
	Value    string `json:"value"`
	ThreadNo int    `json:"threadNo"`
	Index    int    `json:"index"`
}

const Value string = "SomeRandomPayloadString"

func (s *DocumentTests) RunTestThread(ctx *Context, id uint, test TestSettings, threadNo int, results []time.Duration) error {
	dbctx := ctx.openDatabase(s.dbname)

	for k := 0; k < test.NumberOfRequests; k++ {
		entry := [1]MyDocument{{Value, threadNo, k}}
		req_start := time.Now()
		if err := dbctx.insertDocument(CollectionName, entry); err != nil {
			return fmt.Errorf("failed to insert document during test: %v", err)
		}
		results[k] = time.Since(req_start)
	}

	return nil
}

func (DocumentTests) GetTestName(test TestSettings) string {
	name := fmt.Sprintf("doc-insert-c%d-r%d-wc%d-s%d-v%s", test.NumberOfThreads, test.NumberOfServers,
		test.Config.WriteConcern, test.Config.NumberOfShards, test.Config.ReplicationVersion)
	if test.Config.WaitForSync {
		name = name + "-ws"
	}
	return name
}

const CollectionName string = "c"

func (s *DocumentTests) SetupTest(ctx *Context, id uint, test TestSettings) error {
	// create replication 2 database and create a collection
	s.dbname = s.GetTestName(test)
	if err := ctx.createDatabase(s.dbname, &DatabaseOptions{ReplicationVersion: &test.Config.ReplicationVersion}); err != nil {
		return fmt.Errorf("failed to setup test; could not create database %s: %v", s.dbname, err)
	}

	db := ctx.openDatabase(s.dbname)

	opts := CreateCollectionOptions{
		Name:              CollectionName,
		WriteConcern:      test.Config.WriteConcern,
		ReplicationFactor: test.NumberOfServers,
		NumberOfShards:    test.Config.NumberOfShards,
		WaitForSync:       test.Config.WaitForSync,
	}
	if err := db.createCollection(opts); err != nil {
		return fmt.Errorf("failed to create collection: %v", err)
	}

	return nil
}

func (s *DocumentTests) TearDownTest(ctx *Context, id uint) error {
	// drop database
	return ctx.dropDatabase(s.dbname)
}

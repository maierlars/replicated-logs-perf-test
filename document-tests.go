package main

import (
	"fmt"
	"math/rand"
	"time"
)

type DocumentTests struct {
	dbname string
}

type MyDocument struct {
	Value      string `json:"value"`
	ThreadNo   int    `json:"threadNo"`
	Index      int    `json:"index"`
	BatchIndex int    `json:"batchIndex"`
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func (s *DocumentTests) RunTestThread(ctx *Context, id uint, test TestSettings, threadNo int, results []time.Duration) error {
	dbctx := ctx.openDatabase(s.dbname)
	value := randSeq(int(test.Config.DocumentSize))
	entries := make([]MyDocument, test.Config.BatchSize)
	for k := 0; k < test.NumberOfRequests; k++ {
		for j := 0; j < int(test.Config.BatchSize); j++ {
			entries[j] = MyDocument{value, threadNo, k, j}
		}

		req_start := time.Now()
		if err := dbctx.insertDocument(CollectionName, entries); err != nil {
			return fmt.Errorf("failed to insert document during test: %v", err)
		}
		results[k] = time.Since(req_start)
	}

	return nil
}

func (DocumentTests) GetTestName(test TestSettings) string {
	name := fmt.Sprintf("doc-insert-c%d-r%d-wc%d-s%d", test.NumberOfThreads, test.NumberOfServers,
		test.Config.WriteConcern, test.Config.NumberOfShards)
	if test.Config.BatchSize > 1 {
		name = name + fmt.Sprintf("-b%d", test.Config.BatchSize)
	}
	if test.Config.DocumentSize > 64 {
		name = name + fmt.Sprintf("-ds%d", test.Config.DocumentSize)
	}
	if test.Config.WaitForSync {
		name = name + "-ws"
	}
	name = name + fmt.Sprintf("-v%s", test.Config.ReplicationVersion)
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

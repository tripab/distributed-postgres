package main

import (
	"log"
	"net/http"
	"os"
	"path"

	bolt "go.etcd.io/bbolt"

	"github.com/tripab/distributed-postgres/fsm"
	"github.com/tripab/distributed-postgres/queryengine"
	"github.com/tripab/distributed-postgres/raftserver"
	"github.com/tripab/distributed-postgres/wireprotocol"
)

/*
 * Driver
 */
type config struct {
	id       string
	httpPort string
	raftPort string
	pgPort   string
}

func getConfig() config {
	cfg := config{}
	for i, arg := range os.Args[1:] {
		if arg == "--node-id" {
			cfg.id = os.Args[i+2]
			i++
			continue
		}
		if arg == "--http-port" {
			cfg.httpPort = os.Args[i+2]
			i++
			continue
		}
		if arg == "--raft-port" {
			cfg.raftPort = os.Args[i+2]
			i++
			continue
		}
		if arg == "--pg-port" {
			cfg.pgPort = os.Args[i+2]
			i++
			continue
		}
	}

	if cfg.id == "" {
		log.Fatal("Missing required parameter: --node-id")
	}

	if cfg.raftPort == "" {
		log.Fatal("Missing required parameter: --raft-port")
	}

	if cfg.httpPort == "" {
		log.Fatal("Missing required parameter: --http-port")
	}

	if cfg.pgPort == "" {
		log.Fatal("Missing required parameter: --pg-port")
	}

	return cfg
}

func main() {
	cfg := getConfig()

	dataDir := "data"
	err := os.MkdirAll(dataDir, os.ModePerm)
	if err != nil {
		log.Fatalf("Could not create data directory: %s", err)
	}

	db, err := bolt.Open(path.Join(dataDir, "/data"+cfg.id), 0600, nil)
	if err != nil {
		log.Fatalf("Could not open bolt db: %s", err)
	}
	defer db.Close()

	pe := queryengine.NewPgEngine(db)
	// Start off in clean state
	pe.Delete()

	pf := &fsm.PgFsm{pe}
	r, err := raftserver.SetupRaft(path.Join(dataDir, "raft"+cfg.id), cfg.id, "localhost:"+cfg.raftPort, pf)
	if err != nil {
		log.Fatal(err)
	}

	hs := raftserver.HttpServer{r}
	http.HandleFunc("/add-follower", hs.AddFollowerHandler)
	go func() {
		err := http.ListenAndServe(":"+cfg.httpPort, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()

	wireprotocol.RunPgServer(cfg.pgPort, db, r)
}

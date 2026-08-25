// Command server runs the goredis TCP server: it loads any existing
// on-disk state (snapshot + AOF tail), then listens for client connections
// until it receives SIGINT/SIGTERM, at which point it drains in-flight
// connections and shuts down cleanly.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"goredis/internal/engine"
	"goredis/internal/persistence"
	"goredis/internal/server"
	"goredis/internal/store"
)

func main() {
	addr := flag.String("addr", ":6380", "TCP address to listen on")
	dataDir := flag.String("data-dir", "data", "directory for the AOF and snapshot files")
	maxKeys := flag.Int("max-keys", 0, "maximum number of keys before eviction kicks in (0 = unlimited)")
	maxMemoryMB := flag.Int64("max-memory-mb", 0, "approximate maximum memory in MB before eviction kicks in (0 = unlimited)")
	evictionPolicyFlag := flag.String("eviction-policy", "lru", "eviction policy used once a limit above is hit: \"lru\" or \"random\"")
	flag.Parse()

	var policy store.EvictionPolicyType
	switch strings.ToLower(*evictionPolicyFlag) {
	case "lru":
		policy = store.EvictionLRU
	case "random":
		policy = store.EvictionRandom
	default:
		log.Fatalf("unknown -eviction-policy %q (want \"lru\" or \"random\")", *evictionPolicyFlag)
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("creating data dir: %v", err)
	}

	s := store.NewWithConfig(store.Config{
		MaxKeys:        *maxKeys,
		MaxMemoryBytes: *maxMemoryMB * 1024 * 1024,
		EvictionPolicy: policy,
	})

	aof, err := persistence.OpenAOF(filepath.Join(*dataDir, "appendonly.aof"))
	if err != nil {
		log.Fatalf("opening AOF: %v", err)
	}
	snapshotPath := filepath.Join(*dataDir, "dump.rdb")

	eng := engine.New(s, aof, snapshotPath)
	if err := eng.LoadFromDisk(); err != nil {
		log.Fatalf("loading data from disk: %v", err)
	}

	srv := server.New(*addr, eng)

	// Graceful shutdown: stop accepting new connections and let in-flight
	// commands finish (Server.Shutdown), THEN close the engine — closing
	// it first could pull the AOF file / store out from under a
	// still-running command.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down...")
		srv.Shutdown()
	}()

	log.Printf("goredis listening on %s (data dir: %q, eviction policy: %s)", *addr, *dataDir, policy)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("server error: %v", err)
	}

	if err := eng.Close(); err != nil {
		log.Printf("error closing engine: %v", err)
	}
	log.Println("shutdown complete")
}

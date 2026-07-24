package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"

	"github.com/Anmol202005/VScale/internal/tablet/server"
	"github.com/Anmol202005/VScale/internal/tablet/metadata"
	"github.com/Anmol202005/VScale/internal/topology"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, relying on environment variables")
	}

	connString := os.Getenv("DATABASE_URL")
	if connString == "" {
		log.Fatal("DATABASE_URL environment variable is not set")
	}

	meta, err := metadata.LoadFromFlags()
	if err != nil {
		log.Fatalf("invalid tablet metadata: %v", err)
	}
	log.Printf("starting tablet %s (keyspace=%s shard=%s type=%s)",
		meta.Alias(), meta.Keyspace, meta.Shard, meta.Type)

	srv, err := server.NewServer(meta.GRPCPort, connString, meta)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	etcdEndpointsStr := os.Getenv("ETCD_ENDPOINTS")
	if etcdEndpointsStr == "" {
		log.Fatal("ETCD_ENDPOINTS environment variable is not set")
	}
	etcdEndpoints := strings.Split(etcdEndpointsStr, ",")
	for i := range etcdEndpoints {
		etcdEndpoints[i] = strings.TrimSpace(etcdEndpoints[i])
	}

	etcdPrefix := os.Getenv("ETCD_PREFIX")
	if etcdPrefix == "" {
		etcdPrefix = "/vscale/tablets/"
	}

	regCtx, regCancel := context.WithCancel(context.Background())
	defer regCancel()

	etcdCli, err := topology.Register(regCtx, etcdEndpoints, etcdPrefix, meta.Alias(), topology.Tablet{
		Cell:     meta.Cell,
		Keyspace: meta.Keyspace,
		Shard:    meta.Shard,
		Type:     meta.Type.String(),
		Addr:     fmt.Sprintf("%s:%d", meta.Hostname, meta.GRPCPort),
		KeyRangeStart: meta.KeyRangeStart,
        KeyRangeEnd:   meta.KeyRangeEnd,
	}, 10)
	if err != nil {
		log.Fatalf("failed to register with etcd: %v", err)
	}
	defer etcdCli.Close()

	log.Printf("registered tablet %s in etcd under %s%s", meta.Alias(), etcdPrefix, meta.Alias())

	log.Printf("Starting server on port %d...", meta.GRPCPort)
	if err := srv.Start(); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}

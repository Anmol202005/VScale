package main

import (
	"log"
	"os"
	"strconv"
	"github.com/joho/godotenv"
	"github.com/Anmol202005/VScale/internal/tablet/server"
	"github.com/Anmol202005/VScale/internal/tablet/metadata"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, relying on environment variables")
	}

	portStr := os.Getenv("GRPC_PORT")
	if portStr == "" {
		log.Fatal("GRPC_PORT environment variable is not set")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatal("GRPC_PORT environment variable is not set or invalid")
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


	srv, err := server.NewServer(port, connString, meta)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	log.Printf("Starting server on port %d...", port)
	if err := srv.Start(); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}
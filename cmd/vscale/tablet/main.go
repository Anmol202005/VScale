package main

import (
	"log"
	"os"
	"github.com/joho/godotenv"
	"github.com/Anmol202005/VScale/internal/tablet/server"
	"github.com/Anmol202005/VScale/internal/tablet/metadata"
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

	log.Printf("Starting server on port %d...", meta.GRPCPort)
	if err := srv.Start(); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}
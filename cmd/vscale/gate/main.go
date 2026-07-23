package main

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"

	"github.com/Anmol202005/VScale/internal/gate/server"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, relying on environment variables")
	}

	portStr := os.Getenv("VTGATE_PORT")
	if portStr == "" {
		log.Fatal("VTGATE_PORT environment variable is not set")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatal("VTGATE_PORT environment variable is not set or invalid")
	}

	tabletAddr := os.Getenv("TABLET_ADDR")
	if tabletAddr == "" {
		log.Fatal("TABLET_ADDR environment variable is not set")
	}

	listenAddr := ":" + strconv.Itoa(port)

	srv, err := server.New(listenAddr, tabletAddr)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}
	defer srv.Close()

	log.Printf("starting vtgate on port %d, forwarding to tablet at %s", port, tabletAddr)
	if err := srv.Serve(); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}
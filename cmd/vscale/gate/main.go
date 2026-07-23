package main

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"

	"github.com/Anmol202005/VScale/internal/gate/server"
	"github.com/Anmol202005/VScale/internal/topology"
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

	tabletAddrsCSV := os.Getenv("TABLET_ADDRS")
	if tabletAddrsCSV == "" {
		log.Fatal("TABLET_ADDRS environment variable is not set")
	}

	topo, err := topology.LoadFromEnv(tabletAddrsCSV)
	if err != nil {
		log.Fatalf("failed to load topology: %v", err)
	}

	listenAddr := ":" + strconv.Itoa(port)

	srv, err := server.New(listenAddr, topo)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}
	defer srv.Close()

	log.Printf("starting vtgate on port %d, topology tablets: %v", port, topo.GetTablets())
	if err := srv.Serve(); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}

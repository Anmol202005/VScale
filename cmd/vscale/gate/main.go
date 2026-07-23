package main

import (
	"log"
	"os"
	"strconv"
	"strings"

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

	tabletAddrsStr := os.Getenv("TABLET_ADDRS")
	if tabletAddrsStr == "" {
		log.Fatal("TABLET_ADDRS environment variable is not set")
	}
	tabletAddrs := strings.Split(tabletAddrsStr, ",")
	for i := range tabletAddrs {
		tabletAddrs[i] = strings.TrimSpace(tabletAddrs[i])
	}

	listenAddr := ":" + strconv.Itoa(port)

	srv, err := server.New(listenAddr, tabletAddrs)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}
	defer srv.Close()

	log.Printf("starting vtgate on port %d, connected to tablets: %v", port, tabletAddrs)
	if err := srv.Serve(); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}
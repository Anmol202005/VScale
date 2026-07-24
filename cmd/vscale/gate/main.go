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

	vschemaPath := os.Getenv("VSCHEMA_PATH")
	if vschemaPath == "" {
		vschemaPath = "./vschema.json"
	}

	listenAddr := ":" + strconv.Itoa(port)

	srv, err := server.New(listenAddr, etcdEndpoints, etcdPrefix, vschemaPath)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}
	defer srv.Close()

	log.Printf("starting vtgate on port %d, watching etcd prefix %s, vschema %s", port, etcdPrefix, vschemaPath)
	if err := srv.Serve(); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}

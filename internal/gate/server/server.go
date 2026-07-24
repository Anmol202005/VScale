package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/Anmol202005/VScale/proto/tablet"
	"github.com/Anmol202005/VScale/internal/gate/coordinator"
	"github.com/Anmol202005/VScale/internal/gate/gateway"
	"github.com/Anmol202005/VScale/internal/gate/router"
	"github.com/Anmol202005/VScale/internal/gate/session"
	"github.com/Anmol202005/VScale/internal/topology"
	"google.golang.org/grpc"
	"github.com/Anmol202005/VScale/internal/vschema"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	grpcServer  *grpc.Server
	listenAddr  string
	router      *router.Router
	topo        *topology.EtcdTopology
	cancelWatch context.CancelFunc
}

func New(listenAddr string, etcdEndpoints []string, etcdPrefix string, vschemaPath string) (*Server, error) {
	vs, err := vschema.Load(vschemaPath)
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}

	topo, err := topology.NewEtcdTopology(etcdEndpoints, etcdPrefix)
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}

	r := router.NewRouter(vs)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := topo.Watch(ctx, r.Sync); err != nil {
			log.Printf("server: topology watch stopped: %v", err)
		}
	}()

	sm := session.NewManager(300 * time.Second)
	coord := coordinator.New(r, sm)
	gw := gateway.New(r, sm, coord)
	handler := NewVTGateHandler(gw)

	grpcServer := grpc.NewServer()
	pb.RegisterTabletServiceServer(grpcServer, handler)
	
	reflection.Register(grpcServer)

	return &Server{
		grpcServer:  grpcServer,
		listenAddr:  listenAddr,
		router:      r,
		topo:        topo,
		cancelWatch: cancel,
	}, nil
}

func (s *Server) Serve() error {
	lis, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return fmt.Errorf("server: failed to listen on %s: %w", s.listenAddr, err)
	}
	return s.grpcServer.Serve(lis)
}

func (s *Server) Close() error {
	s.cancelWatch()
	s.router.Close()
	return s.topo.Close()
}
package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	pb "github.com/Anmol202005/VScale/proto/tablet"
	"github.com/Anmol202005/VScale/internal/gate/admin"
	"github.com/Anmol202005/VScale/internal/gate/coordinator"
	"github.com/Anmol202005/VScale/internal/gate/gateway"
	"github.com/Anmol202005/VScale/internal/gate/pgwire"
	"github.com/Anmol202005/VScale/internal/gate/router"
	"github.com/Anmol202005/VScale/internal/gate/session"
	"github.com/Anmol202005/VScale/internal/topology"
	"github.com/Anmol202005/VScale/internal/vschema"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	grpcServer  *grpc.Server
	listenAddr  string
	router      *router.Router
	topo        *topology.EtcdTopology
	cancelWatch context.CancelFunc
	pgwire      *pgwire.Server
	admin       *admin.Server
}

func New(listenAddr string, etcdEndpoints []string, etcdPrefix string, vschemaPath string, pgwireAddr string, adminAddr string) (*Server, error) {
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

	pws := pgwire.NewServer(pgwireAddr, gw)
	if err := pws.Listen(); err != nil {
		cancel()
		return nil, fmt.Errorf("server: pgwire: %w", err)
	}

	adm := admin.New(adminAddr, r, sm, gw, topo, vs)

	return &Server{
		grpcServer:  grpcServer,
		listenAddr:  listenAddr,
		router:      r,
		topo:        topo,
		cancelWatch: cancel,
		pgwire:      pws,
		admin:       adm,
	}, nil
}

func (s *Server) Serve() error {
	errCh := make(chan error, 3)

	go func() {
		errCh <- s.pgwire.Serve()
	}()

	go func() {
		lis, err := net.Listen("tcp", s.listenAddr)
		if err != nil {
			errCh <- fmt.Errorf("server: failed to listen on %s: %w", s.listenAddr, err)
			return
		}
		errCh <- s.grpcServer.Serve(lis)
	}()

	go func() {
		errCh <- s.admin.Serve()
	}()

	for i := 0; i < 3; i++ {
		if err := <-errCh; err != nil && err != http.ErrServerClosed {
			return err
		}
	}
	return nil
}

func (s *Server) Close() error {
	s.cancelWatch()
	s.pgwire.Close()
	s.admin.Close()
	s.router.Close()
	return s.topo.Close()
}

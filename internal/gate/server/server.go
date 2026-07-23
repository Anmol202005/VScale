package server

import (
	"fmt"
	"net"

	pb "github.com/Anmol202005/VScale/proto/tablet"
	"github.com/Anmol202005/VScale/internal/gate/client"
	"github.com/Anmol202005/VScale/internal/gate/gateway"
	"github.com/Anmol202005/VScale/internal/gate/router"
	"google.golang.org/grpc"
)

type Server struct {
	grpcServer *grpc.Server
	listenAddr string
	tablets    []*client.TabletClient
}

func New(listenAddr string, tabletAddrs []string) (*Server, error) {
	tablets := make([]*client.TabletClient, 0, len(tabletAddrs))
	for _, addr := range tabletAddrs {
		tc, err := client.NewTabletClient(addr)
		if err != nil {
			for _, t := range tablets {
				t.Close()
			}
			return nil, fmt.Errorf("server: failed to connect to tablet at %s: %w", addr, err)
		}
		tablets = append(tablets, tc)
	}

	r := router.NewRouter(tablets)
	gw := gateway.New(r)
	handler := NewVTGateHandler(gw)

	grpcServer := grpc.NewServer()
	pb.RegisterTabletServiceServer(grpcServer, handler)

	return &Server{
		grpcServer: grpcServer,
		listenAddr: listenAddr,
		tablets:    tablets,
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
	var firstErr error
	for _, t := range s.tablets {
		if err := t.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

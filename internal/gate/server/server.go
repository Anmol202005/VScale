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
	tablet     *client.TabletClient
}

func New(listenAddr, tabletAddr string) (*Server, error) {
	tc, err := client.NewTabletClient(tabletAddr)
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}

	r := router.NewRouter(tc)
	gw := gateway.New(r)
	handler := NewVTGateHandler(gw)

	grpcServer := grpc.NewServer()
	pb.RegisterTabletServiceServer(grpcServer, handler)

	return &Server{
		grpcServer: grpcServer,
		listenAddr: listenAddr,
		tablet:     tc,
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
	return s.tablet.Close()
}

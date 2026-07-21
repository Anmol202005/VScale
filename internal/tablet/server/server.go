package server

import (
	"google.golang.org/grpc"
	"fmt"
	"net"
)
type Server struct {
	grpcServer *grpc.Server
	port int
}

func NewServer(port int) *Server {
	return &Server{
		grpcServer: grpc.NewServer(),
		port: port,
	}
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return err
	}
	return s.grpcServer.Serve(lis)
}

func (s *Server) Stop() {
	s.grpcServer.Stop()
}
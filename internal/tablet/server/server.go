package server

import (
	"google.golang.org/grpc"
	"context"
	"fmt"
	"net"
	"github.com/Anmol202005/VScale/internal/tablet/executor"
	"github.com/Anmol202005/VScale/internal/tablet/pool"
	pb "github.com/Anmol202005/VScale/proto/tablet"
)
type Server struct {
	grpcServer *grpc.Server
	port int

	pool *pool.Pool
}

func NewServer(port int, connString string) *Server {
	grpcServer := grpc.NewServer()
	p, err := pool.NewPool(context.Background(), connString)
	if err != nil {
		panic(fmt.Sprintf("failed to create pool: %v", err))
	}

	handler := NewTabletHandler()
	handler.executor = executor.NewExecutor(p)

	pb.RegisterTabletServiceServer(grpcServer, handler)
	return &Server{
		grpcServer: grpc.NewServer(),
		port: port,
		pool: p,
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
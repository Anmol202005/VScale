package server

import (
	"time"
	"google.golang.org/grpc"
	"context"
	"fmt"
	"net"
	"github.com/Anmol202005/VScale/internal/tablet/executor"
	"github.com/Anmol202005/VScale/internal/tablet/pool"
	pb "github.com/Anmol202005/VScale/proto/tablet"
	"github.com/Anmol202005/VScale/internal/tablet/metadata"
)
type Server struct {
	grpcServer *grpc.Server
	port int

	pool *pool.Pool
}

func NewServer(port int, connString string, meta *metadata.TabletMetadata) (*Server, error) {
	grpcServer := grpc.NewServer()
	p, err := pool.NewPool(context.Background(), connString, meta.MaxConns)
	if err != nil {
		return nil, fmt.Errorf("failed to create pool: %w", err)
	}

	handler := NewTabletHandler(executor.NewExecutor(p, executor.NewTxManager(p, 30*time.Second), meta.Type), meta)

	pb.RegisterTabletServiceServer(grpcServer, handler)
	return &Server{
		grpcServer: grpcServer,
		port: port,
		pool: p,
	}, nil
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
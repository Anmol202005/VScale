package server

import (
	"context"

	pb "github.com/Anmol202005/VScale/proto/tablet"
)

type TabletHandler struct {
	pb.UnimplementedTabletServiceServer
}

func NewTabletHandler() *TabletHandler {
	return &TabletHandler{}
}

func (h *TabletHandler) Execute(
	ctx context.Context,
	req *pb.QueryRequest,
) (*pb.QueryResponse, error) {

	return &pb.QueryResponse{
		Message: "Query received: " + req.Sql,
	}, nil
}

func (h *TabletHandler) Health(
	ctx context.Context,
	req *pb.HealthRequest,
) (*pb.HealthResponse, error) {

	return &pb.HealthResponse{
		Healthy: true,
	}, nil
}
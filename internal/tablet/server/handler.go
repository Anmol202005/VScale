package server

import (
	"context"
	"fmt"

	pb "github.com/Anmol202005/VScale/proto/tablet"
	"github.com/Anmol202005/VScale/internal/tablet/executor"
)

type TabletHandler struct {
	pb.UnimplementedTabletServiceServer

	executor *executor.Executor
}

func NewTabletHandler() *TabletHandler {
	return &TabletHandler{}
}

func (h *TabletHandler) Execute(
	ctx context.Context,
	req *pb.QueryRequest,
) (*pb.QueryResponse, error) {

	result, err := h.executor.Execute(ctx, req.Sql)
	if err != nil {
		return nil, err
	}

	return &pb.QueryResponse{
	Message: fmt.Sprintf("%d rows affected", result.RowsAffected),
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
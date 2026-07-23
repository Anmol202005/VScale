package server

import (
	"context"

	pb "github.com/Anmol202005/VScale/proto/tablet"
	"github.com/Anmol202005/VScale/internal/gate/client"
)

type VTGateHandler struct {
	pb.UnimplementedTabletServiceServer

	tablet *client.TabletClient
}

func NewVTGateHandler(tc *client.TabletClient) *VTGateHandler {
	return &VTGateHandler{tablet: tc}
}

func (h *VTGateHandler) Execute(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	return h.tablet.Execute(ctx, req)
}

func (h *VTGateHandler) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	return h.tablet.Health(ctx, req)
}

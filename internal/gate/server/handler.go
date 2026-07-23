package server

import (
	"context"

	pb "github.com/Anmol202005/VScale/proto/tablet"
	"github.com/Anmol202005/VScale/internal/gate/gateway"
)

type VTGateHandler struct {
	pb.UnimplementedTabletServiceServer

	gw *gateway.Gateway
}

func NewVTGateHandler(gw *gateway.Gateway) *VTGateHandler {
	return &VTGateHandler{gw: gw}
}

func (h *VTGateHandler) Execute(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	return h.gw.Execute(ctx, req)
}

func (h *VTGateHandler) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	return h.gw.Health(ctx, req)
}
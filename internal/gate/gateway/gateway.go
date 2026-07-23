package gateway

import (
	"context"
	"fmt"

	pb "github.com/Anmol202005/VScale/proto/tablet"
	"github.com/Anmol202005/VScale/internal/gate/router"
)

type Gateway struct {
	router *router.Router
}

func New(r *router.Router) *Gateway {
	return &Gateway{router: r}
}

func (g *Gateway) Execute(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	tablet := g.router.Route(req.Sql)
	if tablet == nil {
		return nil, fmt.Errorf("gateway: no tablets available")
	}
	return tablet.Execute(ctx, req)
}

func (g *Gateway) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	tablet := g.router.Route("")
	if tablet == nil {
		return &pb.HealthResponse{Healthy: false}, nil
	}
	return tablet.Health(ctx, req)
}
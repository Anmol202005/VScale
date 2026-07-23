package gateway

import (
	"context"

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
	return tablet.Execute(ctx, req)
}

func (g *Gateway) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	tablet := g.router.Route("")
	return tablet.Health(ctx, req)
}
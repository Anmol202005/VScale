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
	result, err := g.router.Route(req.Sql)
	if err != nil {
		return nil, fmt.Errorf("gateway: routing failed: %w", err)
	}
	if len(result.Tablets) == 0 && !result.TxControl {
		return nil, fmt.Errorf("gateway: no tablets available")
	}

	if result.TxControl {
		return nil, fmt.Errorf("gateway: transaction control statements not yet supported without session tracking")
	}

	if len(result.Tablets) == 1 {
		return result.Tablets[0].Execute(ctx, req)
	}

	merged := &pb.QueryResponse{}
	for _, t := range result.Tablets {
		resp, err := t.Execute(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("gateway: scatter query failed on one tablet: %w", err)
		}
		merged.Results = append(merged.Results, resp.Results...)
	}
	return merged, nil
}

func (g *Gateway) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	result, err := g.router.Route("")
	if err != nil || len(result.Tablets) == 0 {
		return &pb.HealthResponse{Healthy: false}, nil
	}
	return result.Tablets[0].Health(ctx, req)
}
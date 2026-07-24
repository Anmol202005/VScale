package gateway

import (
	"context"
	"fmt"

	ourparser "github.com/Anmol202005/VScale/internal/parser"
	"github.com/Anmol202005/VScale/internal/gate/coordinator"
	"github.com/Anmol202005/VScale/internal/gate/router"
	"github.com/Anmol202005/VScale/internal/gate/session"
	pb "github.com/Anmol202005/VScale/proto/tablet"
)

type Gateway struct {
	router      *router.Router
	sessMgr     *session.Manager
	coordinator *coordinator.Coordinator
}

func New(r *router.Router, sm *session.Manager, coord *coordinator.Coordinator) *Gateway {
	return &Gateway{
		router:      r,
		sessMgr:     sm,
		coordinator: coord,
	}
}

func (g *Gateway) Execute(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	stmts, err := ourparser.Parse(req.Sql)
	if err != nil {
		return nil, fmt.Errorf("gateway: parse failed: %w", err)
	}
	if len(stmts) == 0 {
		return &pb.QueryResponse{}, nil
	}
	stmt := stmts[0]

	if ourparser.IsBegin(stmt) {
		return g.handleBegin(ctx, req.TransactionId)
	}
	if ourparser.IsCommit(stmt) {
		return g.handleCommit(ctx, req.TransactionId)
	}
	if ourparser.IsRollback(stmt) {
		return g.handleRollback(ctx, req.TransactionId)
	}

	if req.TransactionId == 0 {
		return g.executeAutocommit(ctx, req.Sql)
	}

	sess, err := g.sessMgr.Get(req.TransactionId)
	if err != nil {
		return nil, fmt.Errorf("gateway: invalid transaction id %d: %w", req.TransactionId, err)
	}
	return g.coordinator.Execute(ctx, sess, req.Sql)
}

func (g *Gateway) handleBegin(ctx context.Context, txID int64) (*pb.QueryResponse, error) {
	if txID != 0 {
		
		sess, err := g.sessMgr.Get(txID)
		if err != nil {
			return nil, fmt.Errorf("gateway: cannot BEGIN inside invalid session %d: %w", txID, err)
		}
		if sess.GetState() == session.InTransaction {
			return &pb.QueryResponse{TransactionId: txID, Results: []*pb.QueryResult{{Sql: "BEGIN"}}}, nil
		}
		sess.SetState(session.InTransaction)
		sess.ClearParticipants()
		return &pb.QueryResponse{TransactionId: txID, Results: []*pb.QueryResult{{Sql: "BEGIN"}}}, nil
	}

	sess, err := g.coordinator.Begin()
	if err != nil {
		return nil, fmt.Errorf("gateway: failed to begin transaction: %w", err)
	}
	return &pb.QueryResponse{
		TransactionId: sess.ID,
		Results:       []*pb.QueryResult{{Sql: "BEGIN"}},
	}, nil
}

func (g *Gateway) handleCommit(ctx context.Context, txID int64) (*pb.QueryResponse, error) {
	if txID == 0 {
		return nil, fmt.Errorf("gateway: no active transaction to commit")
	}
	sess, err := g.sessMgr.Get(txID)
	if err != nil {
		return nil, fmt.Errorf("gateway: cannot commit invalid transaction %d: %w", txID, err)
	}
	resp, err := g.coordinator.Commit(ctx, sess)
	if err != nil {
		return nil, fmt.Errorf("gateway: commit failed: %w", err)
	}
	resp.TransactionId = 0
	return resp, nil
}

func (g *Gateway) handleRollback(ctx context.Context, txID int64) (*pb.QueryResponse, error) {
	if txID == 0 {
		return nil, fmt.Errorf("gateway: no active transaction to rollback")
	}
	sess, err := g.sessMgr.Get(txID)
	if err != nil {
		return nil, fmt.Errorf("gateway: cannot rollback invalid transaction %d: %w", txID, err)
	}
	resp, err := g.coordinator.Rollback(ctx, sess)
	if err != nil {
		return nil, fmt.Errorf("gateway: rollback failed: %w", err)
	}
	resp.TransactionId = 0
	return resp, nil
}

func (g *Gateway) executeAutocommit(ctx context.Context, sql string) (*pb.QueryResponse, error) {
	result, err := g.router.Route(sql)
	if err != nil {
		return nil, fmt.Errorf("gateway: routing failed: %w", err)
	}
	if len(result.Tablets) == 0 && !result.Scatter {
		return nil, fmt.Errorf("gateway: no tablets available")
	}

	if result.TxControl {
		return nil, fmt.Errorf("gateway: unexpected transaction control in autocommit")
	}

	if len(result.Tablets) == 1 {
		return result.Tablets[0].Execute(ctx, &pb.QueryRequest{Sql: sql})
	}

	merged := &pb.QueryResponse{}
	for _, t := range result.Tablets {
		resp, err := t.Execute(ctx, &pb.QueryRequest{Sql: sql})
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
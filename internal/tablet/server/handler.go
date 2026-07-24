package server

import (
	"context"
	"fmt"

	pb "github.com/Anmol202005/VScale/proto/tablet"
	"github.com/Anmol202005/VScale/internal/tablet/executor"
	"github.com/Anmol202005/VScale/internal/tablet/metadata"
)

type TabletHandler struct {
	pb.UnimplementedTabletServiceServer

	executor *executor.Executor
	meta     *metadata.TabletMetadata
}

func NewTabletHandler(exe *executor.Executor, meta *metadata.TabletMetadata) *TabletHandler {
	return &TabletHandler{
		executor: exe,
		meta:     meta,
	}
}

func (h *TabletHandler) Execute(
	ctx context.Context,
	req *pb.QueryRequest,
) (*pb.QueryResponse, error) {

	results, txID, err := h.executor.ExecuteSQL(ctx, req.Sql, req.TransactionId)
	if err != nil {
		return nil, err
	}

	response := &pb.QueryResponse{
		TransactionId: txID,
	}

	for _, result := range results {
		queryResult := &pb.QueryResult{
			Sql:          result.SQL,
			Columns:      result.Columns,
			RowsAffected: result.RowsAffected,
		}

		for _, row := range result.Rows {
			pbRow := &pb.Row{}

			for _, value := range row {
				pbRow.Values = append(pbRow.Values, fmt.Sprintf("%v", value))
			}

			queryResult.Rows = append(queryResult.Rows, pbRow)
		}

		response.Results = append(response.Results, queryResult)
	}

	return response, nil
}

func (h *TabletHandler) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	if req.TransactionId == 0 {
		return &pb.PrepareResponse{Prepared: true}, nil
	}
	_, err := h.executor.Tx().Query(ctx, req.TransactionId, "SELECT 1")
	if err != nil {
		return &pb.PrepareResponse{Prepared: false, Error: err.Error()}, nil
	}
	return &pb.PrepareResponse{Prepared: true}, nil
}

func (h *TabletHandler) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	if req.TransactionId == 0 {
		return &pb.CommitResponse{Committed: true}, nil
	}
	if err := h.executor.Tx().Commit(ctx, req.TransactionId); err != nil {
		return &pb.CommitResponse{Committed: false, Error: err.Error()}, nil
	}
	return &pb.CommitResponse{Committed: true}, nil
}

func (h *TabletHandler) Rollback(ctx context.Context, req *pb.RollbackRequest) (*pb.RollbackResponse, error) {
	if req.TransactionId == 0 {
		return &pb.RollbackResponse{RolledBack: true}, nil
	}
	if err := h.executor.Tx().Rollback(ctx, req.TransactionId); err != nil {
		return &pb.RollbackResponse{RolledBack: false, Error: err.Error()}, nil
	}
	return &pb.RollbackResponse{RolledBack: true}, nil
}

func (h *TabletHandler) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{
		Healthy:    true,
		Cell:       h.meta.Cell,
		Uid:        h.meta.UID,
		Keyspace:   h.meta.Keyspace,
		Shard:      h.meta.Shard,
		TabletType: h.meta.Type.String(),
		Alias:      h.meta.Alias(),
		MaxConns:   h.meta.MaxConns,
	}, nil
}
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
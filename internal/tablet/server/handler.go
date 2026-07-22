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

	results, err := h.executor.ExecuteSQL(ctx, req.Sql)
	if err != nil {
		return nil, err
	}

	response := &pb.QueryResponse{}

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

func (h *TabletHandler) Health(
	ctx context.Context,
	req *pb.HealthRequest,
) (*pb.HealthResponse, error) {

	return &pb.HealthResponse{
		Healthy: true,
	}, nil
}
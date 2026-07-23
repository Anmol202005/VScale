package client

import (
	"context"
	"fmt"

	pb "github.com/Anmol202005/VScale/proto/tablet"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type TabletClient struct {
	conn   *grpc.ClientConn
	client pb.TabletServiceClient
}

func NewTabletClient(addr string) (*TabletClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("client: failed to dial tablet at %s: %w", addr, err)
	}
	return &TabletClient{
		conn:   conn,
		client: pb.NewTabletServiceClient(conn),
	}, nil
}

func (c *TabletClient) Close() error {
	return c.conn.Close()
}

func (c *TabletClient) Execute(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	return c.client.Execute(ctx, req)
}

func (c *TabletClient) Begin(ctx context.Context) (*pb.QueryResponse, error) {
	return c.Execute(ctx, &pb.QueryRequest{Sql: "BEGIN", TransactionId: 0})
}

func (c *TabletClient) Commit(ctx context.Context, txID int64) (*pb.QueryResponse, error) {
	return c.Execute(ctx, &pb.QueryRequest{Sql: "COMMIT", TransactionId: txID})
}

func (c *TabletClient) Rollback(ctx context.Context, txID int64) (*pb.QueryResponse, error) {
	return c.Execute(ctx, &pb.QueryRequest{Sql: "ROLLBACK", TransactionId: txID})
}

func (c *TabletClient) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	return c.client.Health(ctx, req)
}

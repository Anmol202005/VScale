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

func (c *TabletClient) String() string {
	return c.conn.Target()
}

func (c *TabletClient) Execute(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	return c.client.Execute(ctx, req)
}

func (c *TabletClient) Begin(ctx context.Context) (*pb.QueryResponse, error) {
	return c.Execute(ctx, &pb.QueryRequest{Sql: "BEGIN", TransactionId: 0})
}

// Commit sends the 2PC Commit RPC.
func (c *TabletClient) Commit(ctx context.Context, txID int64) (*pb.CommitResponse, error) {
	return c.client.Commit(ctx, &pb.CommitRequest{TransactionId: txID})
}

// Rollback sends the 2PC Rollback RPC.
func (c *TabletClient) Rollback(ctx context.Context, txID int64) (*pb.RollbackResponse, error) {
	return c.client.Rollback(ctx, &pb.RollbackRequest{TransactionId: txID})
}

// Prepare sends the 2PC Prepare RPC.
func (c *TabletClient) Prepare(ctx context.Context, txID int64) (*pb.PrepareResponse, error) {
	return c.client.Prepare(ctx, &pb.PrepareRequest{TransactionId: txID})
}

func (c *TabletClient) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	return c.client.Health(ctx, req)
}

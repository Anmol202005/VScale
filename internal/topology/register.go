package topology

import (
	"context"
	"encoding/json"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func Register(ctx context.Context, endpoints []string, prefix, alias string, tab Tablet, ttlSeconds int64) (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		return nil, fmt.Errorf("topology: failed to connect to etcd: %w", err)
	}

	lease, err := cli.Grant(ctx, ttlSeconds)
	if err != nil {
		cli.Close()
		return nil, fmt.Errorf("topology: failed to grant lease: %w", err)
	}

	value, err := json.Marshal(tab)
	if err != nil {
		cli.Close()
		return nil, fmt.Errorf("topology: failed to marshal tablet: %w", err)
	}

	key := prefix + alias
	if _, err := cli.Put(ctx, key, string(value), clientv3.WithLease(lease.ID)); err != nil {
		cli.Close()
		return nil, fmt.Errorf("topology: failed to put tablet record: %w", err)
	}

	keepAliveChan, err := cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		cli.Close()
		return nil, fmt.Errorf("topology: failed to start keepalive: %w", err)
	}

	go func() {
		for range keepAliveChan {
		}
	}()

	return cli, nil
}
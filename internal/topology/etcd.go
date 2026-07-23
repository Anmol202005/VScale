package topology

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdTopology struct {
	client *clientv3.Client
	prefix string

	mu      sync.RWMutex
	tablets map[string]Tablet
}

func NewEtcdTopology(endpoints []string, prefix string) (*EtcdTopology, error) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		return nil, fmt.Errorf("topology: failed to connect to etcd: %w", err)
	}
	return &EtcdTopology{
		client:  cli,
		prefix:  prefix,
		tablets: make(map[string]Tablet),
	}, nil
}

func (t *EtcdTopology) Close() error {
	return t.client.Close()
}

func (t *EtcdTopology) Tablets() []Tablet {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]Tablet, 0, len(t.tablets))
	for _, tab := range t.tablets {
		out = append(out, tab)
	}
	return out
}

func (t *EtcdTopology) Watch(ctx context.Context, onUpdate func([]Tablet)) error {
	getResp, err := t.client.Get(ctx, t.prefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("topology: initial get failed: %w", err)
	}

	t.mu.Lock()
	for _, kv := range getResp.Kvs {
		var tab Tablet
		if err := json.Unmarshal(kv.Value, &tab); err != nil {
			log.Printf("topology: skipping invalid tablet record at %s: %v", kv.Key, err)
			continue
		}
		t.tablets[string(kv.Key)] = tab
	}
	t.mu.Unlock()
	onUpdate(t.Tablets())

	watchChan := t.client.Watch(ctx, t.prefix, clientv3.WithPrefix())
	for wresp := range watchChan {
		if wresp.Err() != nil {
			return fmt.Errorf("topology: watch error: %w", wresp.Err())
		}
		changed := false
		t.mu.Lock()
		for _, ev := range wresp.Events {
			key := string(ev.Kv.Key)
			switch ev.Type {
			case clientv3.EventTypePut:
				var tab Tablet
				if err := json.Unmarshal(ev.Kv.Value, &tab); err != nil {
					log.Printf("topology: skipping invalid tablet record at %s: %v", key, err)
					continue
				}
				t.tablets[key] = tab
				changed = true
			case clientv3.EventTypeDelete:
				delete(t.tablets, key)
				changed = true
			}
		}
		t.mu.Unlock()
		if changed {
			onUpdate(t.Tablets())
		}
	}
	return nil
}

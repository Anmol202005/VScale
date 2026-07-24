package router

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"

	ourparser "github.com/Anmol202005/VScale/internal/parser"
	"github.com/Anmol202005/VScale/internal/gate/client"
	"github.com/Anmol202005/VScale/internal/topology"
	"github.com/Anmol202005/VScale/internal/vschema"
)

type QueryType int

const (
	QueryTypeUnknown QueryType = iota
	QueryTypeRead
	QueryTypeWrite
)

type RouteResult struct {
	Tablets   []*client.TabletClient
	Scatter   bool
	TxControl bool
	Write     bool 
}

type shardGroup struct {
	keyspace      string
	shard         string
	keyRangeStart int64
	keyRangeEnd   int64
	primary       topology.Tablet
	replicas      []topology.Tablet
}

func (sg *shardGroup) contains(key int64) bool {
	return key >= sg.keyRangeStart && key < sg.keyRangeEnd
}



type Router struct {
	mu sync.RWMutex

	clients map[string]*client.TabletClient

	shardGroups map[string]*shardGroup

	orderedKeys []string

	replicaCursor atomic.Int64

	vs *vschema.VSchema
}

func NewRouter(vs *vschema.VSchema) *Router {
	return &Router{
		clients:     make(map[string]*client.TabletClient),
		shardGroups: make(map[string]*shardGroup),
		vs:          vs,
	}
}


func (r *Router) Sync(tablets []topology.Tablet) {
	want := make(map[string]topology.Tablet, len(tablets))
	for _, t := range tablets {
		want[t.Addr] = t
	}

	newGroups := make(map[string]*shardGroup)
	for _, t := range want {
		key := t.Keyspace + "/" + t.Shard
		g, ok := newGroups[key]
		if !ok {
			g = &shardGroup{
				keyspace:      t.Keyspace,
				shard:         t.Shard,
				keyRangeStart: t.KeyRangeStart,
				keyRangeEnd:   t.KeyRangeEnd,
			}
			newGroups[key] = g
		}
		if t.Type == "PRIMARY" {
			g.primary = t
		} else {
			g.replicas = append(g.replicas, t)
		}
	}

	for key, g := range newGroups {
		_ = key 
		if g.primary.Addr != "" {
			if g.keyRangeStart == 0 && g.keyRangeEnd == 0 {
				g.keyRangeStart = g.primary.KeyRangeStart
				g.keyRangeEnd = g.primary.KeyRangeEnd
			}
		} else if len(g.replicas) > 0 {
			if g.keyRangeStart == 0 && g.keyRangeEnd == 0 {
				g.keyRangeStart = g.replicas[0].KeyRangeStart
				g.keyRangeEnd = g.replicas[0].KeyRangeEnd
			}
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for addr, c := range r.clients {
		if _, exists := want[addr]; !exists {
			c.Close()
			delete(r.clients, addr)
			log.Printf("router: tablet at %s removed", addr)
		}
	}

	for addr := range want {
		if _, exists := r.clients[addr]; exists {
			continue
		}
		tc, err := client.NewTabletClient(addr)
		if err != nil {
			log.Printf("router: failed to connect to new tablet at %s: %v", addr, err)
			continue
		}
		r.clients[addr] = tc
		log.Printf("router: tablet at %s added", addr)
	}

	r.shardGroups = newGroups
	r.orderedKeys = make([]string, 0, len(newGroups))
	for k := range newGroups {
		r.orderedKeys = append(r.orderedKeys, k)
	}
}

func (r *Router) Route(query string, inTransaction bool) (RouteResult, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.orderedKeys) == 0 {
		return RouteResult{}, nil
	}

	stmts, err := parser.Parse(query)
	if err != nil || len(stmts) == 0 {
		return r.scatterResult(QueryTypeRead, inTransaction), nil
	}
	stmt := stmts[0]

	if ourparser.IsBegin(stmt) || ourparser.IsCommit(stmt) || ourparser.IsRollback(stmt) {
		return RouteResult{TxControl: true}, nil
	}

	isWrite := ourparser.IsWrite(stmt)
	qtype := QueryTypeRead
	if isWrite {
		qtype = QueryTypeWrite
	}

	table, err := ourparser.GetTableName(stmt)
	if err != nil {
		return r.scatterResult(qtype, inTransaction), nil
	}

	if table != "" {
		if shardCol, sharded := r.vs.ShardKeyColumn(table); sharded {
			if valStr, found := ourparser.EqualityValue(stmt, shardCol); found {
				key, err := strconv.ParseInt(valStr, 10, 64)
				if err == nil {
					if target := r.tabletForKeyLocked(key, qtype, inTransaction); target != nil {
						return RouteResult{
							Tablets: []*client.TabletClient{target},
							Write:   isWrite,
						}, nil
					}
				}
			}
		}
	}

	return r.scatterResult(qtype, inTransaction), nil
}

func (r *Router) PickOneForRead(g *shardGroup) *client.TabletClient {
	if len(g.replicas) > 0 {
		idx := int(r.replicaCursor.Add(1)-1) % len(g.replicas)
		return r.clients[g.replicas[idx].Addr]
	}
	return r.clients[g.primary.Addr]
}

func (r *Router) PickOneForWrite(g *shardGroup) *client.TabletClient {
	return r.clients[g.primary.Addr]
}

func (r *Router) tabletForKeyLocked(key int64, qtype QueryType, inTransaction bool) *client.TabletClient {
	for _, g := range r.shardGroups {
		if g.contains(key) {
			if inTransaction || qtype == QueryTypeWrite {
				return r.PickOneForWrite(g)
			}
			return r.PickOneForRead(g)
		}
	}
	return nil
}

func (r *Router) scatterResult(qtype QueryType, inTransaction bool) RouteResult {
	all := make([]*client.TabletClient, 0, len(r.orderedKeys))
	for _, key := range r.orderedKeys {
		g := r.shardGroups[key]
		if g == nil {
			continue
		}
		var tc *client.TabletClient
		if inTransaction || qtype == QueryTypeWrite {
			tc = r.PickOneForWrite(g)
		} else {
			tc = r.PickOneForRead(g)
		}
		if tc != nil {
			all = append(all, tc)
		}
	}
	return RouteResult{Tablets: all, Scatter: true, Write: qtype == QueryTypeWrite}
}

func (r *Router) GetClient(addr string) *client.TabletClient {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.clients[addr]
}

func (r *Router) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.clients {
		c.Close()
	}
}

func (r *Router) GetShardInfo() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := ""
	for _, key := range r.orderedKeys {
		g := r.shardGroups[key]
		if g == nil {
			continue
		}
		out += fmt.Sprintf("shard %s/%s [%d,%d): PRIMARY=%s REPLICAS=%d\n",
			g.keyspace, g.shard, g.keyRangeStart, g.keyRangeEnd,
			g.primary.Addr, len(g.replicas))
	}
	return out
}

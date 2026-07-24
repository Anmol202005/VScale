package router

import (
	"log"
	"strconv"
	"sync"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"

	ourparser "github.com/Anmol202005/VScale/internal/parser"
	"github.com/Anmol202005/VScale/internal/gate/client"
	"github.com/Anmol202005/VScale/internal/topology"
	"github.com/Anmol202005/VScale/internal/vschema"
)

type Router struct {
	mu      sync.RWMutex
	clients map[string]*client.TabletClient
	tablets map[string]topology.Tablet
	order   []string

	vs *vschema.VSchema
}

func NewRouter(vs *vschema.VSchema) *Router {
	return &Router{
		clients: make(map[string]*client.TabletClient),
		tablets: make(map[string]topology.Tablet),
		vs:      vs,
	}
}

func (r *Router) Sync(tablets []topology.Tablet) {
	want := make(map[string]topology.Tablet, len(tablets))
	for _, t := range tablets {
		want[t.Addr] = t
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for addr, c := range r.clients {
		if _, exists := want[addr]; !exists {
			c.Close()
			delete(r.clients, addr)
			delete(r.tablets, addr)
			log.Printf("router: tablet at %s removed", addr)
		}
	}

	for addr, t := range want {
		r.tablets[addr] = t
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

	order := make([]string, 0, len(r.clients))
	for addr := range r.clients {
		order = append(order, addr)
	}
	r.order = order
}

type RouteResult struct {
	Tablets   []*client.TabletClient
	Scatter   bool
	TxControl bool 
}

func (r *Router) Route(query string) (RouteResult, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.order) == 0 {
		return RouteResult{}, nil
	}

	stmts, err := parser.Parse(query)
	if err != nil || len(stmts) == 0 {
		return r.scatterResult(), nil
	}
	stmt := stmts[0]

	if ourparser.IsBegin(stmt) || ourparser.IsCommit(stmt) || ourparser.IsRollback(stmt) {
		return RouteResult{TxControl: true}, nil
	}

	table, err := ourparser.GetTableName(stmt)
	if err != nil {
		return r.scatterResult(), nil
	}
	if table != "" {
		if shardCol, sharded := r.vs.ShardKeyColumn(table); sharded {
			if valStr, found := ourparser.EqualityValue(stmt, shardCol); found {
				key, err := strconv.ParseInt(valStr, 10, 64)
				if err == nil {
					if target := r.tabletForKeyLocked(key); target != "" {
						return RouteResult{Tablets: []*client.TabletClient{r.clients[target]}}, nil
					}
				}
			}
		}
	}

	return r.scatterResult(), nil
}

func (r *Router) tabletForKeyLocked(key int64) string {
	for addr, t := range r.tablets {
		if key >= t.KeyRangeStart && key < t.KeyRangeEnd {
			return addr
		}
	}
	return ""
}

func (r *Router) scatterResult() RouteResult {
	all := make([]*client.TabletClient, 0, len(r.order))
	for _, addr := range r.order {
		all = append(all, r.clients[addr])
	}
	return RouteResult{Tablets: all, Scatter: true}
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

package router

import (
	"log"
	"sync"

	"github.com/Anmol202005/VScale/internal/gate/client"
	"github.com/Anmol202005/VScale/internal/topology"
)

type Router struct {
	mu      sync.RWMutex
	clients map[string]*client.TabletClient
	order   []string
}

func NewRouter() *Router {
	return &Router{clients: make(map[string]*client.TabletClient)}
}

func (r *Router) Sync(tablets []topology.Tablet) {
	want := make(map[string]bool, len(tablets))
	for _, t := range tablets {
		want[t.Addr] = true
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for addr, c := range r.clients {
		if !want[addr] {
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

	order := make([]string, 0, len(r.clients))
	for addr := range r.clients {
		order = append(order, addr)
	}
	r.order = order
}

func (r *Router) Route(query string) *client.TabletClient {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.order) == 0 {
		return nil
	}
	return r.clients[r.order[0]]
}

func (r *Router) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.clients {
		c.Close()
	}
}

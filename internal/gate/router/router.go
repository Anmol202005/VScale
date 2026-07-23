package router

import (
	"github.com/Anmol202005/VScale/internal/gate/client"
)

type Router struct {
	tablets []*client.TabletClient
}

func NewRouter(tablets []*client.TabletClient) *Router {
	return &Router{tablets: tablets}
}

func (r *Router) Route(query string) *client.TabletClient {
	return r.tablets[0]
}

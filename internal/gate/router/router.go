package router

import (
	"github.com/Anmol202005/VScale/internal/gate/client"
)

type Router struct {
	defaultTablet *client.TabletClient
}

func NewRouter(defaultTablet *client.TabletClient) *Router {
	return &Router{defaultTablet: defaultTablet}
}

func (r *Router) Route(query string) *client.TabletClient {
	return r.defaultTablet
}

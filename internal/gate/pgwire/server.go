package pgwire

import (
	"fmt"
	"net"

	"github.com/Anmol202005/VScale/internal/gate/gateway"
)

type Server struct {
	addr    string
	gateway *gateway.Gateway
	ln      net.Listener
}

func NewServer(addr string, gw *gateway.Gateway) *Server {
	return &Server{addr: addr, gateway: gw}
}

func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("pgwire: failed to listen on %s: %w", s.addr, err)
	}
	s.ln = ln
	return nil
}

func (s *Server) Serve() error {
	for {
		nc, err := s.ln.Accept()
		if err != nil {
			if isNetClosed(err) {
				return nil
			}
			return err
		}
		go func(c net.Conn) {
			conn := NewConn(c, s.gateway)
			conn.Run()
		}(nc)
	}
}

func (s *Server) Close() error {
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}

func (s *Server) Addr() string {
	if s.ln != nil {
		return s.ln.Addr().String()
	}
	return s.addr
}

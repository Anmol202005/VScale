package pool

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Pool struct {
	db *pgxpool.Pool
	maxConns int32
}

func NewPool(ctx context.Context, connString string, maxConns int32) (*Pool, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	config.MaxConns = maxConns

	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(ctx); err != nil {
		db.Close()
		return nil, err
	}

	return &Pool{db: db, maxConns: maxConns}, nil
}

func (p *Pool) GetDB() *pgxpool.Pool {
	return p.db
}

func (p *Pool) Close() {
	if p.db != nil {	
	    p.db.Close()
	}
}

func (p *Pool) MaxConns() int32 {
	return p.maxConns
}
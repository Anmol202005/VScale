package executor

import (
	"context"
	"github.com/Anmol202005/VScale/internal/tablet/pool"
)

type Executor struct {
	pool *pool.Pool
}

type Result struct {
	RowsAffected int64
}

func NewExecutor(pool *pool.Pool) *Executor {
	return &Executor{
		pool: pool,
	}
}

func (e *Executor) Execute(ctx context.Context, query string) (*Result, error) {
	tag, err := e.pool.GetDB().Exec(ctx, query)
	if err != nil {
		return nil, err
	}

	return &Result{
		RowsAffected: tag.RowsAffected(),
	}, nil
}
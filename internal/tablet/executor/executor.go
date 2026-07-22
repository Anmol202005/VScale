package executor

import (
	"errors"
	"context"
	"github.com/Anmol202005/VScale/internal/tablet/pool"
	"github.com/Anmol202005/VScale/internal/parser"
	"github.com/Anmol202005/VScale/internal/tablet/metadata"
)

var ErrReadOnly = errors.New("executor: this tablet is read-only (type=" +
	" REPLICA/RDONLY), writes are not permitted")

type Executor struct {
	pool *pool.Pool
	tx *TxManager
	tabletType metadata.TabletType
}

type Result struct {
	SQL		  string
	Columns      []string
	Rows         [][]any
	RowsAffected int64
}

func NewExecutor(pool *pool.Pool, tx *TxManager, tabletType metadata.TabletType) *Executor {
	return &Executor{
		pool: pool,
		tx: tx,
		tabletType: tabletType,
	}
}

func (e *Executor) ExecuteSQL(ctx context.Context, query string, txID int64) ([]Result, int64, error) {
	stmts, err := parser.Parse(query)
	if err != nil {
		return nil, txID, err
	}

	isReadOnlyTablet := e.tabletType == metadata.TabletTypeReplica ||
		e.tabletType == metadata.TabletTypeRdonly

	results := make([]Result, 0)

	for _, stmt := range stmts {
		switch {
		case parser.IsBegin(stmt):
			id, err := e.tx.Begin(ctx)
			if err != nil {
				return nil, txID, err
			}
			txID = id
			results = append(results, Result{SQL: stmt.SQL})
			continue

		case parser.IsCommit(stmt):
			if err := e.tx.Commit(ctx, txID); err != nil {
				return nil, txID, err
			}
			txID = 0
			results = append(results, Result{SQL: stmt.SQL})
			continue

		case parser.IsRollback(stmt):
			if err := e.tx.Rollback(ctx, txID); err != nil {
				return nil, txID, err
			}
			txID = 0
			results = append(results, Result{SQL: stmt.SQL})
			continue
		}

		if isReadOnlyTablet && parser.IsWrite(stmt) {
			return nil, txID, ErrReadOnly
		}

		var result *Result
		if txID != 0 {
			if parser.IsReturning(stmt) {
				result, err = e.tx.Query(ctx, txID, stmt.SQL)
			} else {
				result, err = e.tx.Execute(ctx, txID, stmt.SQL)
			}
		} else {
			if parser.IsReturning(stmt) {
				result, err = e.Query(ctx, stmt.SQL)
			} else {
				result, err = e.Execute(ctx, stmt.SQL)
			}
		}
		if err != nil {
			return nil, txID, err
		}
		results = append(results, *result)
	}

	return results, txID, nil
}
	
func (e *Executor) Execute(ctx context.Context, query string) (*Result, error) {
	tag, err := e.pool.GetDB().Exec(ctx, query)
	if err != nil {
		return nil, err
	}

	return &Result{
		SQL:          query,
		RowsAffected: tag.RowsAffected(),
	}, nil
}

func (e *Executor) Query(ctx context.Context, query string) (*Result, error) {
	rows, err := e.pool.GetDB().Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()

	result := &Result{
		SQL:     query,
		Columns: make([]string, len(fields)),
		Rows:    make([][]any, 0),
		RowsAffected: rows.CommandTag().RowsAffected(),
	}

	for i, field := range fields {
		result.Columns[i] = string(field.Name)
	}

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}

		result.Rows = append(result.Rows, values)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

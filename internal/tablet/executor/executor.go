package executor

import (
	"context"
	"github.com/Anmol202005/VScale/internal/tablet/pool"
	"github.com/Anmol202005/VScale/internal/parser"
)

type Executor struct {
	pool *pool.Pool
}

type Result struct {
	SQL		  string
	Columns      []string
	Rows         [][]any
	RowsAffected int64
}

func NewExecutor(pool *pool.Pool) *Executor {
	return &Executor{
		pool: pool,
	}
}

func (e *Executor) ExecuteSQL(ctx context.Context, query string) ([]Result, error) {
	stmts, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}

	results := make([]Result, 0)
	for _, stmt := range stmts {
		if parser.IsReturning(stmt) {
			result, err := e.Query(ctx, stmt.SQL)
			if err != nil {
				return nil, err
			}
			results = append(results, *result)
			continue
		}
		result, err := e.Execute(ctx, stmt.SQL)
		if err != nil {
			return nil, err
		}
		results = append(results, *result)
	}
	return results, nil
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

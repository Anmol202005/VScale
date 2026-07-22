package executor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Anmol202005/VScale/internal/tablet/pool"
)

var (
	ErrNoSuchTransaction = errors.New("executor: no such transaction")
)

type transaction struct {
	id       int64
	conn     *pgxpool.Conn
	tx       pgx.Tx
	mu       sync.Mutex
	lastUsed time.Time
}

type TxManager struct {
	pool *pool.Pool

	mu     sync.Mutex
	txns   map[int64]*transaction
	nextID int64

	idleTimeout time.Duration
	stopReaper  chan struct{}
}

func NewTxManager(p *pool.Pool, idleTimeout time.Duration) *TxManager {
	if idleTimeout <= 0 {
		idleTimeout = 30 * time.Second
	}
	m := &TxManager{
		pool:        p,
		txns:        make(map[int64]*transaction),
		idleTimeout: idleTimeout,
		stopReaper:  make(chan struct{}),
	}
	go m.reapLoop()
	return m
}

func (m *TxManager) Close() {
	close(m.stopReaper)
}

func (m *TxManager) Begin(ctx context.Context) (int64, error) {
	conn, err := m.pool.GetDB().Acquire(ctx)
	if err != nil {
		return 0, err
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		conn.Release()
		return 0, err
	}

	id := atomic.AddInt64(&m.nextID, 1)

	m.mu.Lock()
	m.txns[id] = &transaction{id: id, conn: conn, tx: tx, lastUsed: time.Now()}
	m.mu.Unlock()

	return id, nil
}

func (m *TxManager) get(id int64) (*transaction, error) {
	m.mu.Lock()
	t, ok := m.txns[id]
	m.mu.Unlock()
	if !ok {
		return nil, ErrNoSuchTransaction
	}
	return t, nil
}

func (m *TxManager) remove(id int64) {
	m.mu.Lock()
	t, ok := m.txns[id]
	if ok {
		delete(m.txns, id)
	}
	m.mu.Unlock()

	if ok {
		t.conn.Release()
	}
}

func (m *TxManager) Execute(ctx context.Context, id int64, query string) (*Result, error) {
	t, err := m.get(id)
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastUsed = time.Now()

	tag, err := t.tx.Exec(ctx, query)
	if err != nil {
		return nil, err
	}

	return &Result{SQL: query, RowsAffected: tag.RowsAffected()}, nil
}

func (m *TxManager) Query(ctx context.Context, id int64, query string) (*Result, error) {
	t, err := m.get(id)
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastUsed = time.Now()

	rows, err := t.tx.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	result := &Result{
		SQL:     query,
		Columns: make([]string, len(fields)),
		Rows:    make([][]any, 0),
	}
	for i, f := range fields {
		result.Columns[i] = string(f.Name)
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
	result.RowsAffected = rows.CommandTag().RowsAffected()

	return result, nil
}

func (m *TxManager) Commit(ctx context.Context, id int64) error {
	t, err := m.get(id)
	if err != nil {
		return err
	}

	t.mu.Lock()
	err = t.tx.Commit(ctx)
	t.mu.Unlock()

	m.remove(id)
	return err
}

func (m *TxManager) Rollback(ctx context.Context, id int64) error {
	t, err := m.get(id)
	if err != nil {
		return err
	}

	t.mu.Lock()
	err = t.tx.Rollback(ctx)
	t.mu.Unlock()

	m.remove(id)
	return err
}

func (m *TxManager) reapLoop() {
	ticker := time.NewTicker(m.idleTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopReaper:
			return
		case <-ticker.C:
			m.reapOnce()
		}
	}
}

func (m *TxManager) reapOnce() {
	var stale []int64

	m.mu.Lock()
	now := time.Now()
	for id, t := range m.txns {
		if now.Sub(t.lastUsed) > m.idleTimeout {
			stale = append(stale, id)
		}
	}
	m.mu.Unlock()

	for _, id := range stale {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = m.Rollback(ctx, id)
		cancel()
	}
}

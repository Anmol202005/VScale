package health

import (
	"context"
	"time"
	"fmt"
	"github.com/Anmol202005/VScale/internal/tablet/pool"
	"github.com/Anmol202005/VScale/internal/tablet/executor"
)

type Status struct {
	Healthy   bool
	Reason    string
	CheckedAt time.Time
}

type Monitor struct {
	pool *pool.Pool
	tx   *executor.TxManager
}

func NewMonitor(p *pool.Pool, tx *executor.TxManager) *Monitor {
	return &Monitor{pool: p, tx: tx}
}

func (m *Monitor) Check(ctx context.Context, p *pool.Pool) Status {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := m.pool.GetDB().Ping(ctx); err != nil {
		return Status{
			Healthy: false,
			Reason: "postgres unreachable: " + err.Error(),
			CheckedAt: time.Now(),
		}
	}
    stat := m.pool.GetDB().Stat()
    acquired := stat.AcquiredConns()
	maxOpenTxns := p.MaxConns() 

	if acquired > maxOpenTxns {
		return Status{Healthy: false, Reason: fmt.Sprintf("too many open transactions: %d", acquired), CheckedAt: time.Now()}
	}

	return Status{
		Healthy: true,
		CheckedAt: time.Now(),
	}
}
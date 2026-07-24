package coordinator

import (
	"context"
	"fmt"
	"sync"

	pb "github.com/Anmol202005/VScale/proto/tablet"
	"github.com/Anmol202005/VScale/internal/gate/client"
	"github.com/Anmol202005/VScale/internal/gate/router"
	"github.com/Anmol202005/VScale/internal/gate/session"
)

var (
	ErrNoTabletsAvailable = fmt.Errorf("coordinator: no tablets available")
	ErrPrepareFailed      = fmt.Errorf("coordinator: PREPARE failed on one or more shards")
	ErrCommitFailed       = fmt.Errorf("coordinator: COMMIT failed on one or more shards")
	ErrRollbackFailed     = fmt.Errorf("coordinator: ROLLBACK failed on one or more shards")
)

type Coordinator struct {
	router  *router.Router
	sessMgr *session.Manager
}

func New(r *router.Router, sm *session.Manager) *Coordinator {
	return &Coordinator{router: r, sessMgr: sm}
}


func (c *Coordinator) Begin() (*session.Session, error) {
	sess := c.sessMgr.Create()
	sess.SetState(session.InTransaction)
	return sess, nil
}

func (c *Coordinator) Commit(ctx context.Context, sess *session.Session) (*pb.QueryResponse, error) {
	if sess.GetState() != session.InTransaction {
		return nil, fmt.Errorf("coordinator: cannot commit, session state=%d", sess.GetState())
	}

	shardCount := sess.ShardCount()
	if shardCount == 0 {
		sess.SetState(session.Committed)
		return &pb.QueryResponse{Results: []*pb.QueryResult{{Sql: "COMMIT"}}}, nil
	}

	if shardCount == 1 {
		for addr, txID := range sess.ParticipatingShardsMap() {
			tc := c.router.GetClient(addr)
			if tc == nil {
				return nil, fmt.Errorf("coordinator: tablet %s not available for commit", addr)
			}
			resp, err := tc.Commit(ctx, txID)
			if err != nil {
				_ = c.rollbackAll(ctx, sess)
				return nil, fmt.Errorf("coordinator: commit on %s failed: %w", addr, err)
			}
			if resp != nil && !resp.Committed {
				_ = c.rollbackAll(ctx, sess)
				return nil, fmt.Errorf("coordinator: commit on %s rejected: %s", addr, resp.Error)
			}
		}
		sess.SetState(session.Committed)
		sess.ClearParticipants()
		return &pb.QueryResponse{Results: []*pb.QueryResult{{Sql: "COMMIT"}}}, nil
	}

	if err := c.prepareAll(ctx, sess); err != nil {
		_ = c.rollbackAll(ctx, sess)
		return nil, err
	}

	if err := c.commitAll(ctx, sess); err != nil {
		return nil, err
	}

	sess.SetState(session.Committed)
	sess.ClearParticipants()
	return &pb.QueryResponse{Results: []*pb.QueryResult{{Sql: "COMMIT"}}}, nil
}

func (c *Coordinator) Rollback(ctx context.Context, sess *session.Session) (*pb.QueryResponse, error) {
	if sess.GetState() != session.InTransaction {
		return nil, fmt.Errorf("coordinator: cannot rollback, session state=%d", sess.GetState())
	}
	_ = c.rollbackAll(ctx, sess)
	sess.SetState(session.RolledBack)
	sess.ClearParticipants()
	return &pb.QueryResponse{Results: []*pb.QueryResult{{Sql: "ROLLBACK"}}}, nil
}

func (c *Coordinator) Execute(ctx context.Context, sess *session.Session, sql string) (*pb.QueryResponse, error) {
	if sess.GetState() != session.InTransaction {
		return nil, fmt.Errorf("coordinator: session not in transaction (state=%d)", sess.GetState())
	}

	result, err := c.router.Route(sql)
	if err != nil {
		return nil, fmt.Errorf("coordinator: routing failed: %w", err)
	}
	if len(result.Tablets) == 0 && !result.Scatter {
		return nil, ErrNoTabletsAvailable
	}

	for _, tc := range result.Tablets {
		if tc == nil {
			continue
		}
		addr := tc.String()
		if !sess.IsParticipating(addr) {
			txID, err := c.beginOnTablet(ctx, tc)
			if err != nil {
				_ = c.rollbackAll(ctx, sess)
				return nil, fmt.Errorf("coordinator: failed to begin on shard %s: %w", addr, err)
			}
			sess.SetLocalTxID(addr, txID)
		}
	}

	if len(result.Tablets) == 1 && !result.Scatter {
		tc := result.Tablets[0]
		addr := tc.String()
		localTxID, _ := sess.GetLocalTxID(addr)
		return tc.Execute(ctx, &pb.QueryRequest{
			Sql:           sql,
			TransactionId: localTxID,
		})
	}

	return c.executeScatter(ctx, sess, sql, result.Tablets)
}

func (c *Coordinator) beginOnTablet(ctx context.Context, tc *client.TabletClient) (int64, error) {
	resp, err := tc.Begin(ctx)
	if err != nil {
		return 0, err
	}
	return resp.TransactionId, nil
}

func (c *Coordinator) executeScatter(ctx context.Context, sess *session.Session, sql string, tablets []*client.TabletClient) (*pb.QueryResponse, error) {
	var mu sync.Mutex
	merged := &pb.QueryResponse{}
	var firstErr error

	for _, tc := range tablets {
		if tc == nil {
			continue
		}
		addr := tc.String()
		localTxID, _ := sess.GetLocalTxID(addr)
		resp, err := tc.Execute(ctx, &pb.QueryRequest{
			Sql:           sql,
			TransactionId: localTxID,
		})
		if err != nil {
			mu.Lock()
			if firstErr == nil {
				firstErr = fmt.Errorf("coordinator: scatter query failed on shard %s: %w", addr, err)
			}
			mu.Unlock()
			continue
		}
		mu.Lock()
		merged.Results = append(merged.Results, resp.Results...)
		mu.Unlock()
	}

	if firstErr != nil {
		_ = c.rollbackAll(ctx, sess)
		return nil, firstErr
	}
	return merged, nil
}

func (c *Coordinator) prepareAll(ctx context.Context, sess *session.Session) error {
	shards := sess.ParticipatingShardsMap()
	if len(shards) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(shards))

	for addr, txID := range shards {
		wg.Add(1)
		go func(a string, id int64) {
			defer wg.Done()
			tc := c.router.GetClient(a)
			if tc == nil {
				errChan <- fmt.Errorf("prepare on %s: client not available", a)
				return
			}
			resp, err := tc.Prepare(ctx, id)
			if err != nil {
				errChan <- fmt.Errorf("prepare on %s: %w", a, err)
				return
			}
			if !resp.Prepared {
				errChan <- fmt.Errorf("prepare on %s rejected: %s", a, resp.Error)
			}
		}(addr, txID)
	}
	wg.Wait()
	close(errChan)

	for e := range errChan {
		if e != nil {
			return fmt.Errorf("%w: %w", ErrPrepareFailed, e)
		}
	}
	return nil
}

func (c *Coordinator) commitAll(ctx context.Context, sess *session.Session) error {
	shards := sess.ParticipatingShardsMap()
	if len(shards) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(shards))

	for addr, txID := range shards {
		wg.Add(1)
		go func(a string, id int64) {
			defer wg.Done()
			tc := c.router.GetClient(a)
			if tc == nil {
				errChan <- fmt.Errorf("commit on %s: client not available", a)
				return
			}
			resp, err := tc.Commit(ctx, id)
			if err != nil {
				errChan <- fmt.Errorf("commit on %s: %w", a, err)
				return
			}
			if !resp.Committed {
				errChan <- fmt.Errorf("commit on %s rejected: %s", a, resp.Error)
			}
		}(addr, txID)
	}
	wg.Wait()
	close(errChan)

	for e := range errChan {
		if e != nil {
			return fmt.Errorf("%w: %w", ErrCommitFailed, e)
		}
	}
	return nil
}

func (c *Coordinator) rollbackAll(ctx context.Context, sess *session.Session) error {
	shards := sess.ParticipatingShardsMap()
	if len(shards) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(shards))

	for addr, txID := range shards {
		wg.Add(1)
		go func(a string, id int64) {
			defer wg.Done()
			tc := c.router.GetClient(a)
			if tc == nil {
				errChan <- fmt.Errorf("rollback on %s: client not available", a)
				return
			}
			resp, err := tc.Rollback(ctx, id)
			if err != nil {
				errChan <- fmt.Errorf("rollback on %s: %w", a, err)
				return
			}
			if !resp.RolledBack {
				errChan <- fmt.Errorf("rollback on %s: %s", a, resp.Error)
			}
		}(addr, txID)
	}
	wg.Wait()
	close(errChan)

	for e := range errChan {
		if e != nil {
			return fmt.Errorf("%w: %w", ErrRollbackFailed, e)
		}
	}
	return nil
}

package session

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrSessionNotFound = errors.New("session: no such session")
)

type State int

const (
	Idle State = iota
	InTransaction
	Committed
	RolledBack
)


type Session struct {
	ID         int64
	State      State
	ShardTxIDs map[string]int64

	mu       sync.RWMutex
	lastUsed time.Time
}

type Manager struct {
	mu       sync.RWMutex
	sessions map[int64]*Session
	nextID   int64

	idleTimeout time.Duration
	stopReaper  chan struct{}
}

func NewManager(idleTimeout time.Duration) *Manager {
	if idleTimeout <= 0 {
		idleTimeout = 300 * time.Second
	}
	m := &Manager{
		sessions:    make(map[int64]*Session),
		idleTimeout: idleTimeout,
		stopReaper:  make(chan struct{}),
	}
	go m.reapLoop()
	return m
}

func (m *Manager) Close() {
	close(m.stopReaper)
}

func (m *Manager) Create() *Session {
	id := atomic.AddInt64(&m.nextID, 1)
	s := &Session{
		ID:         id,
		State:      Idle,
		ShardTxIDs: make(map[string]int64),
		lastUsed:   time.Now(),
	}
	m.mu.Lock()
	m.sessions[id] = s
	m.mu.Unlock()
	return s
}

func (m *Manager) Get(id int64) (*Session, error) {
	m.mu.RLock()
	s, ok := m.sessions[id]
	m.mu.RUnlock()
	if !ok {
		return nil, ErrSessionNotFound
	}
	s.Touch()
	return s, nil
}

func (m *Manager) Remove(id int64) {
	m.mu.Lock()
	delete(m.sessions, id)
	m.mu.Unlock()
}

func (s *Session) Touch() {
	s.mu.Lock()
	s.lastUsed = time.Now()
	s.mu.Unlock()
}

func (s *Session) GetState() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.State
}

func (s *Session) SetState(state State) {
	s.mu.Lock()
	s.State = state
	s.mu.Unlock()
}

func (s *Session) GetLocalTxID(addr string) (int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.ShardTxIDs[addr]
	return id, ok
}

func (s *Session) SetLocalTxID(addr string, id int64) {
	s.mu.Lock()
	s.ShardTxIDs[addr] = id
	s.mu.Unlock()
}

func (s *Session) IsParticipating(addr string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.ShardTxIDs[addr]
	return ok
}

func (s *Session) ShardCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.ShardTxIDs)
}

func (s *Session) ParticipatingShardsList() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.ShardTxIDs))
	for addr := range s.ShardTxIDs {
		out = append(out, addr)
	}
	return out
}

func (s *Session) ClearParticipants() {
	s.mu.Lock()
	s.ShardTxIDs = make(map[string]int64)
	s.mu.Unlock()
}

func (s *Session) ParticipatingShardsMap() map[string]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]int64, len(s.ShardTxIDs))
	for k, v := range s.ShardTxIDs {
		out[k] = v
	}
	return out
}

func (s *Session) IsStale(timeout time.Duration) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return time.Since(s.lastUsed) > timeout
}

func (m *Manager) reapLoop() {
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

func (m *Manager) reapOnce() {
	var stale []int64
	m.mu.RLock()
	for id, s := range m.sessions {
		if s.IsStale(m.idleTimeout) {
			stale = append(stale, id)
		}
	}
	m.mu.RUnlock()
	for _, id := range stale {
		m.Remove(id)
	}
}

func (s *Session) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("Session{id=%d, state=%d, shards=%v}", s.ID, s.State, s.ShardTxIDs)
}

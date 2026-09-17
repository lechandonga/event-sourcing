package snapshot

import (
	"context"
	"sync"
	"time"
)

type memKey struct{ t, id string }

// MemoryStore keeps snapshots in process memory.
type MemoryStore struct {
	mu   sync.RWMutex
	data map[memKey]*Snapshot
}

// NewMemoryStore creates an empty in-memory snapshot store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[memKey]*Snapshot)}
}

// Save implements Store.
func (m *MemoryStore) Save(_ context.Context, s *Snapshot) error {
	cp := *s
	if cp.TakenAt.IsZero() {
		cp.TakenAt = time.Now().UTC()
	}
	cp.Checksum = checksum(&cp)
	m.mu.Lock()
	m.data[memKey{s.AggregateType, s.AggregateID}] = &cp
	m.mu.Unlock()
	return nil
}

// Load implements Store.
func (m *MemoryStore) Load(_ context.Context, aggregateType, aggregateID string) (*Snapshot, error) {
	m.mu.RLock()
	s, ok := m.data[memKey{aggregateType, aggregateID}]
	m.mu.RUnlock()
	if !ok {
		return nil, &ErrSnapshotNotFound{AggregateType: aggregateType, AggregateID: aggregateID}
	}
	cp := *s
	if err := verify(&cp); err != nil {
		return nil, err
	}
	return &cp, nil
}

// Delete implements Store.
func (m *MemoryStore) Delete(_ context.Context, aggregateType, aggregateID string) error {
	m.mu.Lock()
	delete(m.data, memKey{aggregateType, aggregateID})
	m.mu.Unlock()
	return nil
}

// CorruptTest overwrites the state bytes without refreshing the checksum.
// Test-only helper for exercising corruption fallback.
func (m *MemoryStore) CorruptTest(aggregateType, aggregateID string, badState []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.data[memKey{aggregateType, aggregateID}]; ok {
		s.State = badState // checksum deliberately left stale
	}
}

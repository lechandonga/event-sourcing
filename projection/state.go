package projection

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lechandonga/event-sourcing/internal/safeio"
)

// StateStore persists the read model state itself, alongside the checkpoint.
// A model that implements Stateful is snapshotted whenever its checkpoint
// commits; on projector startup the newest state is restored BEFORE
// incremental processing resumes, so a restarted process does not need a
// full rebuild.
//
// The saved position is embedded in the state record. A state behind the
// checkpoint simply means events are replayed from the state's position; a
// state ahead of the checkpoint is refused.
type StateStore interface {
	SaveState(ctx context.Context, name string, record *StateRecord) error
	LoadState(ctx context.Context, name string) (*StateRecord, error)
}

// StateRecord is the persisted model state bundle.
type StateRecord struct {
	Name      string          `json:"name"`
	Position  int64           `json:"position"`
	UpdatedAt time.Time       `json:"updated_at"`
	State     json.RawMessage `json:"state"`
}

// ErrStateNotFound means no persisted model state exists yet.
type ErrStateNotFound struct{ Name string }

func (e *ErrStateNotFound) Error() string {
	return "projection: no model state for " + e.Name
}

// Stateful is implemented by read models that can snapshot/restore their own
// state. It is optional: models without it still work, but after a process
// restart they must be rebuilt via Projector.Rebuild.
type Stateful interface {
	ReadModel
	// MarshalState serializes the current model state.
	MarshalState() ([]byte, error)
	// UnmarshalState restores model state previously produced by
	// MarshalState.
	UnmarshalState(data []byte) error
}

// MemoryStateStore keeps model states in process memory.
type MemoryStateStore struct {
	mu   sync.RWMutex
	data map[string]*StateRecord
}

// NewMemoryStateStore creates an empty in-memory state store.
func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{data: make(map[string]*StateRecord)}
}

func (m *MemoryStateStore) SaveState(_ context.Context, name string, rec *StateRecord) error {
	rec.Name = name
	cp := *rec
	cp.State = append(json.RawMessage(nil), rec.State...)
	m.mu.Lock()
	m.data[rec.Name] = &cp
	m.mu.Unlock()
	return nil
}

func (m *MemoryStateStore) LoadState(_ context.Context, name string) (*StateRecord, error) {
	m.mu.RLock()
	rec, ok := m.data[name]
	m.mu.RUnlock()
	if !ok {
		return nil, &ErrStateNotFound{Name: name}
	}
	cp := *rec
	return &cp, nil
}

// FileStateStore persists model states as one atomic JSON file per
// projection under dir/model-states/.
type FileStateStore struct {
	dir string
}

// NewFileStateStore creates/opens a model-state directory.
func NewFileStateStore(dir string) *FileStateStore {
	return &FileStateStore{dir: filepath.Join(dir, "model-states")}
}

func (f *FileStateStore) path(name string) string {
	return filepath.Join(f.dir, safeio.SafeName(name)+".json")
}

func (f *FileStateStore) SaveState(_ context.Context, name string, rec *StateRecord) error {
	rec.Name = name
	if rec.UpdatedAt.IsZero() {
		rec.UpdatedAt = time.Now().UTC()
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("projection: marshal model state: %w", err)
	}
	return safeio.WriteFileAtomic(f.path(rec.Name), data, 0o644)
}

func (f *FileStateStore) LoadState(_ context.Context, name string) (*StateRecord, error) {
	data, err := os.ReadFile(f.path(name))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &ErrStateNotFound{Name: name}
		}
		return nil, fmt.Errorf("projection: read model state: %w", err)
	}
	var rec StateRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("projection: corrupted model state for %s: %w", name, err)
	}
	if rec.Name != name || rec.Position < 0 {
		return nil, fmt.Errorf("projection: model state for %s failed identity check", name)
	}
	return &rec, nil
}

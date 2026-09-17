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

// Checkpoint marks how far a projection has durably processed the global
// event stream.
type Checkpoint struct {
	// Name identifies the projection (independent projections keep
	// independent checkpoints).
	Name string
	// Position is the last global event position processed successfully.
	// 0 means "nothing processed yet".
	Position int64
	// UpdatedAt is the last successful commit time.
	UpdatedAt time.Time
}

// CheckpointStore persists checkpoints locally.
type CheckpointStore interface {
	Load(ctx context.Context, name string) (*Checkpoint, error)
	Save(ctx context.Context, cp *Checkpoint) error
}

// ErrCheckpointNotFound means no checkpoint exists yet => start from zero.
type ErrCheckpointNotFound struct{ Name string }

func (e *ErrCheckpointNotFound) Error() string {
	return "projection: no checkpoint for " + e.Name
}

// MemoryCheckpointStore is an in-process checkpoint store.
type MemoryCheckpointStore struct {
	mu   sync.RWMutex
	data map[string]*Checkpoint
}

// NewMemoryCheckpointStore creates an empty store.
func NewMemoryCheckpointStore() *MemoryCheckpointStore {
	return &MemoryCheckpointStore{data: make(map[string]*Checkpoint)}
}

func (m *MemoryCheckpointStore) Load(_ context.Context, name string) (*Checkpoint, error) {
	m.mu.RLock()
	cp, ok := m.data[name]
	m.mu.RUnlock()
	if !ok {
		return nil, &ErrCheckpointNotFound{Name: name}
	}
	cp2 := *cp
	return &cp2, nil
}

func (m *MemoryCheckpointStore) Save(_ context.Context, cp *Checkpoint) error {
	cp2 := *cp
	if cp2.UpdatedAt.IsZero() {
		cp2.UpdatedAt = time.Now().UTC()
	}
	m.mu.Lock()
	m.data[cp.Name] = &cp2
	m.mu.Unlock()
	return nil
}

type diskCheckpoint struct {
	Name      string    `json:"name"`
	Position  int64     `json:"position"`
	UpdatedAt time.Time `json:"updated_at"`
}

// FileCheckpointStore persists one JSON file per projection under
// dir/checkpoints/, written atomically.
type FileCheckpointStore struct {
	dir string
}

// NewFileCheckpointStore creates/opens a checkpoint directory.
func NewFileCheckpointStore(dir string) *FileCheckpointStore {
	return &FileCheckpointStore{dir: filepath.Join(dir, "checkpoints")}
}

func (f *FileCheckpointStore) path(name string) string {
	return filepath.Join(f.dir, safeio.SafeName(name)+".json")
}

func (f *FileCheckpointStore) Load(_ context.Context, name string) (*Checkpoint, error) {
	data, err := os.ReadFile(f.path(name))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &ErrCheckpointNotFound{Name: name}
		}
		return nil, fmt.Errorf("projection: read checkpoint: %w", err)
	}
	var d diskCheckpoint
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("projection: corrupted checkpoint for %s: %w", name, err)
	}
	if d.Name != name || d.Position < 0 {
		return nil, fmt.Errorf("projection: corrupted checkpoint for %s: identity/position invalid", name)
	}
	return &Checkpoint{Name: d.Name, Position: d.Position, UpdatedAt: d.UpdatedAt}, nil
}

func (f *FileCheckpointStore) Save(_ context.Context, cp *Checkpoint) error {
	d := diskCheckpoint{Name: cp.Name, Position: cp.Position, UpdatedAt: cp.UpdatedAt}
	if d.UpdatedAt.IsZero() {
		d.UpdatedAt = time.Now().UTC()
	}
	data, err := json.MarshalIndent(&d, "", "  ")
	if err != nil {
		return fmt.Errorf("projection: marshal checkpoint: %w", err)
	}
	return safeio.WriteFileAtomic(f.path(cp.Name), data, 0o644)
}

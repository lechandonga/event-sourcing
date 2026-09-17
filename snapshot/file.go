package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lechandonga/event-sourcing/internal/safeio"
)

// FileStore persists one JSON file per aggregate under dir/snapshots/.
// Writes are atomic (temp file + fsync + rename); reads verify the checksum.
type FileStore struct {
	dir string
}

// NewFileStore creates/opens a file-backed snapshot directory.
func NewFileStore(dir string) *FileStore {
	return &FileStore{dir: filepath.Join(dir, "snapshots")}
}

func (f *FileStore) path(aggregateType, aggregateID string) string {
	return filepath.Join(f.dir, safeio.SafeName(aggregateType)+"__"+safeio.SafeName(aggregateID)+".json")
}

// Save implements Store.
func (f *FileStore) Save(_ context.Context, s *Snapshot) error {
	cp := *s
	cp.Checksum = checksum(&cp)
	data, err := json.MarshalIndent(&cp, "", "  ")
	if err != nil {
		return fmt.Errorf("snapshot: marshal: %w", err)
	}
	if err := safeio.WriteFileAtomic(f.path(s.AggregateType, s.AggregateID), data, 0o644); err != nil {
		return err
	}
	return nil
}

// Load implements Store. Missing files => ErrSnapshotNotFound; checksum or
// JSON failures => CorruptionError so callers fall back to full replay.
func (f *FileStore) Load(_ context.Context, aggregateType, aggregateID string) (*Snapshot, error) {
	data, err := os.ReadFile(f.path(aggregateType, aggregateID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &ErrSnapshotNotFound{AggregateType: aggregateType, AggregateID: aggregateID}
		}
		return nil, fmt.Errorf("snapshot: read file: %w", err)
	}
	var s Snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, &CorruptionError{AggregateType: aggregateType, AggregateID: aggregateID,
			Reason: "invalid json: " + err.Error()}
	}
	if s.AggregateType == "" || s.AggregateID == "" || s.Version < 1 || len(s.State) == 0 {
		return nil, &CorruptionError{AggregateType: aggregateType, AggregateID: aggregateID,
			Reason: "mandatory fields missing"}
	}
	if err := verify(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

// Delete implements Store.
func (f *FileStore) Delete(_ context.Context, aggregateType, aggregateID string) error {
	err := os.Remove(f.path(aggregateType, aggregateID))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("snapshot: delete: %w", err)
	}
	return nil
}

// CorruptTest overwrites the snapshot file with bytes whose embedded
// checksum cannot match. Test-only helper for corruption-fallback coverage.
func (f *FileStore) CorruptTest(aggregateType, aggregateID string) error {
	garbage := []byte(`{"aggregate_type":"agg","aggregate_id":"a1","version":7,` +
		`"state_schema_version":1,"state":"e30=","checksum":1}`)
	return os.WriteFile(f.path(aggregateType, aggregateID), garbage, 0o644)
}

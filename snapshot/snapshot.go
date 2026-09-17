// Package snapshot provides aggregate state snapshots and local snapshot
// stores. A snapshot shortens replay by recording aggregate state at a known
// stream version; events AFTER that version are always replayed.
//
// Every snapshot carries enough information to locate its exact position in
// the event stream:
//
//	AggregateType, AggregateID  - which stream
//	Version                     - version of the last event included
//	SchemaVersion               - version of the serialized state format
//
// plus a CRC32 checksum so a torn/corrupted snapshot file is detected and
// causes a safe fallback to full sequential replay instead of applying
// events on top of a bad state.
package snapshot

import (
	"context"
	"encoding/json"
	"time"
)

// Snapshot is the persisted state record.
type Snapshot struct {
	AggregateType string `json:"aggregate_type"`
	AggregateID   string `json:"aggregate_id"`
	// Version is the stream version of the last event folded into State.
	// Replay must resume strictly after this version; it must never cause an
	// event to be skipped.
	Version       int             `json:"version"`
	SchemaVersion int             `json:"state_schema_version"`
	TakenAt       time.Time       `json:"taken_at"`
	State         json.RawMessage `json:"state"`
	// Checksum is CRC32-IEEE over the JSON of all fields above. Filled by the
	// store when saving; verified when loading.
	Checksum uint32 `json:"checksum"`
}

// Store persists snapshots locally. Save replaces any previous snapshot for
// the aggregate (snapshots are a compaction, not a history).
type Store interface {
	Save(ctx context.Context, s *Snapshot) error
	Load(ctx context.Context, aggregateType, aggregateID string) (*Snapshot, error)
	// Delete removes a snapshot (used when a snapshot is proven unusable).
	Delete(ctx context.Context, aggregateType, aggregateID string) error
}

// ErrSnapshotNotFound is returned by Load when no snapshot exists. Callers
// treat this as "replay the whole stream", never as a fatal error.
type ErrSnapshotNotFound struct {
	AggregateType string
	AggregateID   string
}

func (e *ErrSnapshotNotFound) Error() string {
	return "snapshot: none found for " + e.AggregateType + "/" + e.AggregateID
}

// CorruptionError means a snapshot exists but cannot be trusted.
type CorruptionError struct {
	AggregateType string
	AggregateID   string
	Reason        string
}

func (e *CorruptionError) Error() string {
	return "snapshot: corrupted snapshot for " + e.AggregateType + "/" + e.AggregateID + ": " + e.Reason
}

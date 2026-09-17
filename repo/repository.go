// Package repo implements the aggregate repository: loading aggregates with
// snapshot acceleration and safe fallback, and saving pending events with
// optimistic concurrency plus periodic snapshot compaction ("history
// trimming" on the read path; the event log itself is never rewritten).
package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lechandonga/event-sourcing/eventstore"
	"github.com/lechandonga/event-sourcing/snapshot"
)

// Aggregate is the contract application aggregates implement.
type Aggregate interface {
	// AggregateID/AggregateType identify the stream.
	AggregateID() string
	AggregateType() string
	// Version is the stream version the instance was loaded to (0 for a new
	// aggregate that has never been saved).
	Version() int
	// SeekVersion moves the in-memory version cursor to v after state was
	// restored from a snapshot taken at v. Returning an error makes the
	// repository ignore the snapshot and replay the full stream.
	SeekVersion(v int) error

	// ApplyEvent folds one *already upcast* event into aggregate state and
	// advances the in-memory version. Used both for replay and for applying
	// freshly produced events after a successful save.
	ApplyEvent(env eventstore.Envelope, decoded any) error

	// PendingEvents returns events produced by commands since the last save
	// WITHOUT clearing them.
	PendingEvents() []eventstore.UncommittedEvent
	// PullPending returns pending events and clears the queue.
	PullPending() []eventstore.UncommittedEvent

	// MarshalState serializes current state for a snapshot.
	MarshalState() (stateSchemaVersion int, data []byte, err error)
	// UnmarshalState restores state from a snapshot. The repository then
	// replays events with Version > snapshot.Version.
	UnmarshalState(schemaVersion int, data []byte) error
}

// Factory creates a new empty aggregate of one type.
type Factory func(id string) Aggregate

// Options configures a Repository.
type Options struct {
	// SnapshotInterval: save a snapshot every N new versions. 0 disables
	// snapshots on save (they can still be loaded). Default 20.
	SnapshotInterval int
	// MaxSnapshotAge: a snapshot older than this is treated as stale and
	// triggers full replay (re-snapshot happens on the next save). Zero
	// disables age checks.
	MaxSnapshotAge time.Duration
	// OnSnapshotWarning receives non-fatal snapshot problems (checksum
	// mismatch, unreadable state...) so they can be logged/alerted. Load
	// always falls back to full replay on these.
	OnSnapshotWarning func(aggregateType, aggregateID string, err error)
}

// LoadInfo reports how an aggregate load was materialized.
type LoadInfo struct {
	// Mode is "snapshot+replay", "full_replay" or "new".
	Mode string
	// SnapshotVersion is the version the used snapshot was taken at (0 when
	// no snapshot used).
	SnapshotVersion int
	// ReplayedFrom is the first stream version replayed (1 for full replay).
	ReplayedFrom int
	// ReplayedCount is number of events replayed during this load.
	ReplayedCount int
}

// Repository combines an event store, a snapshot store and an event registry.
type Repository struct {
	store     eventstore.EventStore
	snapshots snapshot.Store
	registry  upcaster
	factory   Factory
	opts      Options
}

// upcaster is the subset of *event.Registry the repository needs.
type upcaster interface {
	Decode(eventType string, schemaVersion int, raw json.RawMessage) (any, error)
}

// New builds a repository. registry may be nil when payloads are not decoded
// into typed values.
func New(store eventstore.EventStore, snapshots snapshot.Store, registry upcaster, factory Factory, opts Options) *Repository {
	if opts.SnapshotInterval == 0 {
		opts.SnapshotInterval = 20
	}
	return &Repository{store: store, snapshots: snapshots, registry: registry, factory: factory, opts: opts}
}

// Load returns the aggregate rebuilt to its current committed version.
//
// Loading strategy, in order:
//  1. If a valid, fresh snapshot exists: restore state at snapshot.Version
//     and replay only events after it.
//  2. If the snapshot is missing, stale, corrupted, points behind/gap or the
//     state cannot be deserialized: discard it for this load and replay ALL
//     events from version 1. No event is ever skipped.
//  3. If the stream is empty, a new aggregate instance is returned.
func (r *Repository) Load(ctx context.Context, id string) (Aggregate, LoadInfo, error) {
	agg := r.factory(id)
	aggType := agg.AggregateType()
	info := LoadInfo{Mode: "new", ReplayedFrom: 1}

	snap, snapErr := r.snapshots.Load(ctx, aggType, id)
	switch {
	case snapErr == nil:
		if usable := r.snapshotUsable(ctx, snap, aggType, id); usable {
			if err := agg.UnmarshalState(snap.SchemaVersion, snap.State); err == nil {
				// The restored state already reflects events 1..snap.Version;
				// the aggregate must know that so subsequent replay/appends
				// continue at the right version.
				if err := agg.SeekVersion(snap.Version); err != nil {
					r.warn(aggType, id, fmt.Errorf("cannot seek to snapshot version %d: %w", snap.Version, err))
				} else {
					info.Mode = "snapshot+replay"
					info.SnapshotVersion = snap.Version
					info.ReplayedFrom = snap.Version + 1
				}
			} else {
				r.warn(aggType, id, fmt.Errorf("snapshot state rejected: %w", err))
			}
		}
		// In any other case: fall through to full replay.
	default:
		var nf *snapshot.ErrSnapshotNotFound
		if !errors.As(snapErr, &nf) {
			r.warn(aggType, id, snapErr)
		}
	}

	fromVersion := info.SnapshotVersion
	events, err := r.store.ReadStream(ctx, aggType, id, eventstore.ReadOptions{FromVersion: fromVersion})
	if err != nil {
		return nil, info, fmt.Errorf("repo: read stream %s/%s: %w", aggType, id, err)
	}
	if info.Mode == "new" && len(events) > 0 {
		info.Mode = "full_replay"
	}
	if info.Mode == "snapshot+replay" {
		// Snapshot fallback guard: if the snapshot claims a version but the
		// tail does not line up (missing event or duplicate), refuse the
		// accelerated path and replay everything from scratch.
		if len(events) > 0 && events[0].Version != fromVersion+1 {
			r.warn(aggType, id, fmt.Errorf("snapshot at v%d not adjacent to history (next v%d); falling back",
				fromVersion, events[0].Version))
			return r.fullReplay(ctx, id)
		}
	}
	if err := r.applyAll(ctx, agg, events); err != nil {
		// Any replay error on a snapshot-accelerated path means we cannot
		// trust that partial state: rebuild from version 1.
		if info.Mode == "snapshot+replay" {
			r.warn(aggType, id, fmt.Errorf("replay after snapshot failed: %w; falling back", err))
			return r.fullReplay(ctx, id)
		}
		return nil, info, err
	}
	info.ReplayedCount = len(events)
	return agg, info, nil
}

func (r *Repository) fullReplay(ctx context.Context, id string) (Aggregate, LoadInfo, error) {
	agg := r.factory(id)
	aggType := agg.AggregateType()
	events, err := r.store.ReadStream(ctx, aggType, id, eventstore.ReadOptions{})
	if err != nil {
		return nil, LoadInfo{}, fmt.Errorf("repo: full replay read %s/%s: %w", aggType, id, err)
	}
	if err := r.applyAll(ctx, agg, events); err != nil {
		return nil, LoadInfo{}, err
	}
	mode := "new"
	if len(events) > 0 {
		mode = "full_replay"
	}
	return agg, LoadInfo{
		Mode: mode, SnapshotVersion: 0, ReplayedFrom: 1, ReplayedCount: len(events),
	}, nil
}

func (r *Repository) applyAll(ctx context.Context, agg Aggregate, events []eventstore.Envelope) error {
	for i := range events {
		env := events[i]
		var decoded any
		if r.registry != nil {
			v, err := r.registry.Decode(env.EventType, env.SchemaVersion, env.Payload)
			if err != nil {
				return fmt.Errorf("repo: decode %s (v%d) at v%d: %w", env.EventType, env.SchemaVersion, env.Version, err)
			}
			decoded = v
		}
		if err := agg.ApplyEvent(env, decoded); err != nil {
			return fmt.Errorf("repo: apply %s at v%d: %w", env.EventType, env.Version, err)
		}
	}
	return nil
}

func (r *Repository) snapshotUsable(ctx context.Context, snap *snapshot.Snapshot, aggType, id string) bool {
	if snap.AggregateType != aggType || snap.AggregateID != id || snap.Version < 1 {
		r.warn(aggType, id, errors.New("snapshot identity/version invalid"))
		return false
	}
	if r.opts.MaxSnapshotAge > 0 && time.Since(snap.TakenAt) > r.opts.MaxSnapshotAge {
		r.warn(aggType, id, fmt.Errorf("snapshot age %s exceeds %s", time.Since(snap.TakenAt), r.opts.MaxSnapshotAge))
		return false
	}
	// The snapshot must not point beyond the actual committed history.
	cur, err := r.store.StreamVersion(ctx, aggType, id)
	if err != nil {
		r.warn(aggType, id, fmt.Errorf("stream version lookup failed: %w", err))
		return false
	}
	if snap.Version > cur {
		r.warn(aggType, id, fmt.Errorf("snapshot v%d ahead of stream v%d", snap.Version, cur))
		return false
	}
	return true
}

// Save appends pending events with optimistic concurrency (expected version
// is the aggregate's loaded version), folds the committed events back into
// the aggregate, and compacts into a snapshot when the version crosses an
// interval boundary.
//
// On a version conflict nothing is appended and the aggregate keeps its
// pending events; the caller should reload and decide whether to rebase.
func (r *Repository) Save(ctx context.Context, agg Aggregate) ([]eventstore.Envelope, LoadInfo, error) {
	aggType := agg.AggregateType()
	id := agg.AggregateID()
	expected := agg.Version()
	pending := agg.PullPending()
	if len(pending) == 0 {
		return nil, LoadInfo{}, nil
	}

	committed, err := r.store.Append(ctx, aggType, id, expected, pending)
	if err != nil {
		// ConflictError is returned as-is (distinguishable reason).
		return nil, LoadInfo{}, err
	}
	if err := r.applyAll(ctx, agg, committed); err != nil {
		return committed, LoadInfo{}, fmt.Errorf("repo: fold committed events: %w", err)
	}

	newVersion := agg.Version()
	saved := false
	if r.opts.SnapshotInterval > 0 && newVersion >= r.opts.SnapshotInterval &&
		newVersion/r.opts.SnapshotInterval > expected/r.opts.SnapshotInterval {
		if err := r.snapshotAggregate(ctx, agg); err != nil {
			// Snapshot failure must not fail the already-committed write.
			r.warn(aggType, id, fmt.Errorf("post-save snapshot failed: %w", err))
		} else {
			saved = true
		}
	}
	info := LoadInfo{Mode: "saved", ReplayedCount: len(committed), ReplayedFrom: expected + 1}
	if saved {
		info.SnapshotVersion = newVersion
	}
	return committed, info, nil
}

func (r *Repository) snapshotAggregate(ctx context.Context, agg Aggregate) error {
	schemaVersion, data, err := agg.MarshalState()
	if err != nil {
		return err
	}
	snap := &snapshot.Snapshot{
		AggregateType: agg.AggregateType(),
		AggregateID:   agg.AggregateID(),
		Version:       agg.Version(),
		SchemaVersion: schemaVersion,
		TakenAt:       time.Now().UTC(),
		State:         data,
	}
	return r.snapshots.Save(ctx, snap)
}

func (r *Repository) warn(aggType, id string, err error) {
	if r.opts.OnSnapshotWarning != nil {
		r.opts.OnSnapshotWarning(aggType, id, err)
	}
}

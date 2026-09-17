// Package eventstore defines the append-only event store used for event
// sourcing and provides two fully local implementations:
//
//   - MemoryEventStore: in-process storage guarded by a RWMutex.
//   - FileEventStore: a durable append-only JSON-line log on the local disk.
//
// Neither implementation talks to any external service.
//
// Every committed event receives two orderings:
//
//   - Stream version: the 1-based, gap-free position of the event inside its
//     aggregate stream. Used for optimistic concurrency control.
//   - Global position: the 1-based, gap-free, strictly increasing position of
//     the event across all aggregates. Used by projections/checkpoints.
//
// Appends are serialized process-wide, so both orderings are total and
// deterministic relative to commit order.
package eventstore

import (
	"context"
	"encoding/json"
	"time"
)

// Envelope is the *committed* representation of an event. Payload is stored
// verbatim as it was appended: upgrading a historical event to a newer
// structure happens while reading/replaying, never by rewriting storage.
type Envelope struct {
	// EventID is a client-supplied unique id used for idempotent appends and
	// deduplication. It is never empty in a committed envelope.
	EventID string
	// AggregateType/AggregateID identify the stream the event belongs to.
	AggregateType string
	AggregateID   string
	// Version is the 1-based position of this event within its stream.
	Version int
	// Position is the 1-based global position across all streams.
	Position int64
	// EventType is the logical payload type (e.g. "bank.account.opened").
	EventType string
	// SchemaVersion is the payload schema revision (>= 1).
	SchemaVersion int
	// Timestamp is assigned by the store at append time.
	Timestamp time.Time
	// Payload is the raw, immutable event data.
	Payload json.RawMessage
	// Metadata is optional auxiliary data (correlation ids, subject, ...).
	Metadata map[string]string
}

// UncommittedEvent is an event a caller wants to append. The stream version
// and global position are assigned by the store.
type UncommittedEvent struct {
	EventID       string
	EventType     string
	SchemaVersion int
	Payload       json.RawMessage
	Timestamp     time.Time
	Metadata      map[string]string
}

// ReadOptions controls stream reads.
type ReadOptions struct {
	// FromVersion is the exclusive lower bound: only events with
	// Version > FromVersion are returned. Use 0 for the whole stream.
	FromVersion int
	// MaxEvents caps the number of returned events; <= 0 means unlimited.
	MaxEvents int
}

// EventStore is the storage contract used by aggregates, repositories and
// projections.
type EventStore interface {
	// Append commits events for one aggregate stream atomically: either all
	// events are committed (with consecutive versions) or none are.
	//
	// expectedVersion is the version the caller based its decision on:
	//   expectedVersion == 0 means the stream must not exist yet;
	//   expectedVersion > 0  means the current stream version must equal it.
	//
	// On mismatch a *ConflictError is returned and nothing is appended.
	Append(ctx context.Context, aggregateType, aggregateID string, expectedVersion int, events []UncommittedEvent) ([]Envelope, error)

	// ReadStream returns committed events of one stream ordered by version.
	ReadStream(ctx context.Context, aggregateType, aggregateID string, opts ReadOptions) ([]Envelope, error)

	// ReadAll returns committed events ordered by global position:
	//   fromPosition is exclusive (use 0 to start at the beginning);
	//   maxEvents <= 0 means a store-defined default batch size.
	ReadAll(ctx context.Context, fromPosition int64, maxEvents int) ([]Envelope, error)

	// StreamVersion returns the current version of a stream (0 if absent).
	StreamVersion(ctx context.Context, aggregateType, aggregateID string) (int, error)

	// LastPosition returns the current highest global position (0 if empty).
	LastPosition(ctx context.Context) (int64, error)
}

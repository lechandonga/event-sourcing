package eventstore

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

type streamKey struct {
	aggregateType string
	aggregateID   string
}

// MemoryEventStore keeps all events in process memory. It is safe for
// concurrent use and fully implements optimistic concurrency control.
type MemoryEventStore struct {
	mu       sync.RWMutex
	streams  map[streamKey][]Envelope
	all      []Envelope
	position int64
}

// NewMemoryEventStore creates an empty in-memory store.
func NewMemoryEventStore() *MemoryEventStore {
	return &MemoryEventStore{streams: make(map[streamKey][]Envelope)}
}

func validateBatch(events []UncommittedEvent) (*ConflictError, string) {
	if len(events) == 0 {
		return &ConflictError{Reason: ConflictEmptyAppend, Message: "no events to append"}, ""
	}
	for i := range events {
		ev := &events[i]
		if ev.EventType == "" {
			return &ConflictError{Reason: ConflictInvalidEvent, EventID: ev.EventID,
				Message: "event type is empty"}, ev.EventID
		}
		if ev.SchemaVersion < 1 {
			return &ConflictError{Reason: ConflictInvalidEvent, EventID: ev.EventID,
				Message: "schema version must be >= 1"}, ev.EventID
		}
		if len(json.RawMessage(ev.Payload)) == 0 {
			return &ConflictError{Reason: ConflictInvalidEvent, EventID: ev.EventID,
				Message: "event payload is empty"}, ev.EventID
		}
		if !json.Valid(ev.Payload) {
			return &ConflictError{Reason: ConflictInvalidEvent, EventID: ev.EventID,
				Message: "event payload is not valid JSON"}, ev.EventID
		}
	}
	return nil, ""
}

func cloneEnvelopes(in []Envelope) []Envelope {
	if in == nil {
		return nil
	}
	out := make([]Envelope, len(in))
	copy(out, in)
	return out
}

func cloneMetadata(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	c := make(map[string]string, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

// Append implements EventStore.
func (s *MemoryEventStore) Append(_ context.Context, aggregateType, aggregateID string, expectedVersion int, events []UncommittedEvent) ([]Envelope, error) {
	if ce, id := validateBatch(events); ce != nil {
		ce.AggregateType = aggregateType
		ce.AggregateID = aggregateID
		ce.ExpectedVersion = expectedVersion
		ce.EventID = id
		return nil, ce
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := streamKey{aggregateType, aggregateID}
	current := s.streams[key]
	actual := len(current)

	if expectedVersion == 0 {
		if actual != 0 {
			return nil, &ConflictError{
				Reason: ConflictStreamExists, AggregateType: aggregateType, AggregateID: aggregateID,
				ExpectedVersion: 0, ActualVersion: actual,
				Message: "stream already exists; load it and append at its current version",
			}
		}
	} else {
		if actual == 0 {
			return nil, &ConflictError{
				Reason: ConflictStreamNotFound, AggregateType: aggregateType, AggregateID: aggregateID,
				ExpectedVersion: expectedVersion, ActualVersion: 0,
				Message: "stream does not exist",
			}
		}
		if actual != expectedVersion {
			return nil, &ConflictError{
				Reason: ConflictVersionMismatch, AggregateType: aggregateType, AggregateID: aggregateID,
				ExpectedVersion: expectedVersion, ActualVersion: actual,
				Message: "stream was modified concurrently; reload and retry",
			}
		}
	}

	// Duplicate event-id check across the whole store. Ids are global, so an
	// id already committed must never be committed a second time.
	for i := range events {
		id := events[i].EventID
		if id == "" {
			return nil, &ConflictError{
				Reason: ConflictInvalidEvent, AggregateType: aggregateType, AggregateID: aggregateID,
				ExpectedVersion: expectedVersion, ActualVersion: actual,
				Message: "event id is empty",
			}
		}
		for _, e := range s.all {
			if e.EventID == id {
				return nil, &ConflictError{
					Reason: ConflictDuplicateEventID, AggregateType: aggregateType, AggregateID: aggregateID,
					ExpectedVersion: expectedVersion, ActualVersion: actual, EventID: id,
					Message: "event with this id is already committed",
				}
			}
		}
	}
	// Reject duplicated ids inside the same batch as well.
	seen := make(map[string]struct{}, len(events))
	for i := range events {
		if _, ok := seen[events[i].EventID]; ok {
			return nil, &ConflictError{
				Reason: ConflictDuplicateEventID, AggregateType: aggregateType, AggregateID: aggregateID,
				ExpectedVersion: expectedVersion, ActualVersion: actual, EventID: events[i].EventID,
				Message: "duplicate event id inside the same batch",
			}
		}
		seen[events[i].EventID] = struct{}{}
	}

	now := time.Now().UTC()
	committed := make([]Envelope, 0, len(events))
	nextVersion := actual
	for i := range events {
		ev := &events[i]
		nextVersion++
		s.position++
		ts := ev.Timestamp
		if ts.IsZero() {
			ts = now
		}
		en := Envelope{
			EventID:       ev.EventID,
			AggregateType: aggregateType,
			AggregateID:   aggregateID,
			Version:       nextVersion,
			Position:      s.position,
			EventType:     ev.EventType,
			SchemaVersion: ev.SchemaVersion,
			Timestamp:     ts,
			Payload:       append(json.RawMessage(nil), ev.Payload...),
			Metadata:      cloneMetadata(ev.Metadata),
		}
		s.streams[key] = append(s.streams[key], en)
		s.all = append(s.all, en)
		committed = append(committed, en)
	}
	return committed, nil
}

// ReadStream implements EventStore.
func (s *MemoryEventStore) ReadStream(_ context.Context, aggregateType, aggregateID string, opts ReadOptions) ([]Envelope, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	src := s.streams[streamKey{aggregateType, aggregateID}]
	var out []Envelope
	for _, e := range src {
		if e.Version <= opts.FromVersion {
			continue
		}
		if opts.MaxEvents > 0 && len(out) >= opts.MaxEvents {
			break
		}
		out = append(out, e)
	}
	return cloneEnvelopes(out), nil
}

// ReadAll implements EventStore.
func (s *MemoryEventStore) ReadAll(_ context.Context, fromPosition int64, maxEvents int) ([]Envelope, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Envelope
	for _, e := range s.all {
		if e.Position <= fromPosition {
			continue
		}
		if maxEvents > 0 && len(out) >= maxEvents {
			break
		}
		out = append(out, e)
	}
	return cloneEnvelopes(out), nil
}

// StreamVersion implements EventStore.
func (s *MemoryEventStore) StreamVersion(_ context.Context, aggregateType, aggregateID string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.streams[streamKey{aggregateType, aggregateID}]), nil
}

// LastPosition implements EventStore.
func (s *MemoryEventStore) LastPosition(_ context.Context) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.position, nil
}

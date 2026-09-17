// Package event implements event structure evolution.
//
// Historical events are stored forever in their original schema and are
// upgraded ("upcast") to the current schema every time they are read for a
// replay or projection. The committed bytes in the event store are never
// rewritten.
//
// Evolution rules enforced here:
//
//  1. Additive only between major revisions: a newer schema may add fields
//     and reinterpret defaults; stored payloads are never modified.
//  2. Each (EventType, fromVersion -> toVersion) pair has one deterministic
//     Upcaster. Upcasters are pure functions over JSON so the same historical
//     event always upgrades to the same current event.
//  3. New fields missing from old payloads get a documented default supplied
//     by the upcaster (see package bank for the concrete default catalog).
//  4. Extra unknown fields present in newer payloads are preserved untouched
//     during upcast and ignored by decoders ("be liberal in what you read").
//  5. Forward skipping is impossible by construction: Decode rejects an event
//     whose schema version is newer than anything the binary understands.
package event

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Upcaster upgrades one payload from version V to V+1.
//
// Implementations MUST be pure: no I/O, no time/ randomness, no mutation of
// the input. They should decode the known fields, fill stable defaults for
// missing fields, and re-marshal without dropping unknown fields.
type Upcaster func(raw json.RawMessage) (json.RawMessage, error)

type upcastKey struct {
	eventType string
	version   int // from-version
}

// Decoder unmarshals an already-upcast payload into a concrete Go value.
type Decoder func(raw json.RawMessage) (any, error)

// Registry knows the current schema version, the upcast chain and the
// decoders for every event type a binary can handle.
type Registry struct {
	mu         sync.RWMutex
	upcasters  map[upcastKey]Upcaster
	decoders   map[string]Decoder
	currentV   map[string]int
	eventTypes map[string]struct{}
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		upcasters:  make(map[upcastKey]Upcaster),
		decoders:   make(map[string]Decoder),
		currentV:   make(map[string]int),
		eventTypes: make(map[string]struct{}),
	}
}

// Register declares an event type: currentVersion is the schema revision the
// code now produces, decode converts an up-to-current payload into the Go
// value application code consumes.
func (r *Registry) Register(eventType string, currentVersion int, decode Decoder) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if currentVersion < 1 {
		panic(fmt.Sprintf("event: current version for %q must be >= 1", eventType))
	}
	if _, exists := r.eventTypes[eventType]; exists {
		panic(fmt.Sprintf("event: event type %q registered twice", eventType))
	}
	r.eventTypes[eventType] = struct{}{}
	r.currentV[eventType] = currentVersion
	r.decoders[eventType] = decode
}

// AddUpcaster registers the deterministic upgrade step from version ->
// version+1. Steps form a gap-free chain from 1 up to currentVersion-1.
func (r *Registry) AddUpcaster(eventType string, fromVersion int, up Upcaster) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if fromVersion < 1 {
		panic(fmt.Sprintf("event: upcaster for %q must start at version >= 1", eventType))
	}
	if up == nil {
		panic(fmt.Sprintf("event: nil upcaster for %q v%d", eventType, fromVersion))
	}
	k := upcastKey{eventType, fromVersion}
	if _, exists := r.upcasters[k]; exists {
		panic(fmt.Sprintf("event: upcaster for %q v%d registered twice", eventType, fromVersion))
	}
	r.upcasters[k] = up
}

// Validate checks that every registered type has a complete upcast chain
// from version 1 to currentVersion. Call it once at startup after wiring.
func (r *Registry) Validate() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for typ, cur := range r.currentV {
		for v := 1; v < cur; v++ {
			if _, ok := r.upcasters[upcastKey{typ, v}]; !ok {
				return fmt.Errorf("event: missing upcaster for %q v%d -> v%d", typ, v, v+1)
			}
		}
	}
	return nil
}

// Upcast runs the full upgrade chain from fromVersion to the current version
// registered for eventType. Raw is never modified; upgraded JSON is returned.
// Payloads already at the current version are returned verbatim.
func (r *Registry) Upcast(eventType string, fromVersion int, raw json.RawMessage) (json.RawMessage, error) {
	r.mu.RLock()
	cur, known := r.currentV[eventType]
	r.mu.RUnlock()
	if !known {
		// Unknown event type: cannot upgrade or decode. Callers that only
		// route envelopes can ignore the error, but decoding is an explicit,
		// distinguishable failure.
		return nil, &UnknownEventError{EventType: eventType, Version: fromVersion}
	}
	if fromVersion > cur {
		return nil, &NewerSchemaError{EventType: eventType, Have: fromVersion, Want: cur}
	}
	out := append(json.RawMessage(nil), raw...)
	for v := fromVersion; v < cur; v++ {
		r.mu.RLock()
		up := r.upcasters[upcastKey{eventType, v}]
		r.mu.RUnlock()
		if up == nil {
			return nil, fmt.Errorf("event: missing upcaster for %q v%d -> v%d", eventType, v, v+1)
		}
		next, err := up(out)
		if err != nil {
			return nil, fmt.Errorf("event: upcast %q v%d -> v%d failed: %w", eventType, v, v+1, err)
		}
		out = next
	}
	return out, nil
}

// Decode upcasts the payload to the current schema and decodes it into the
// registered Go value.
func (r *Registry) Decode(eventType string, schemaVersion int, raw json.RawMessage) (any, error) {
	up, err := r.Upcast(eventType, schemaVersion, raw)
	if err != nil {
		return nil, err
	}
	r.mu.RLock()
	cur := r.currentV[eventType]
	dec := r.decoders[eventType]
	r.mu.RUnlock()
	_ = cur
	if dec == nil {
		return nil, &UnknownEventError{EventType: eventType, Version: schemaVersion}
	}
	v, err := dec(up)
	if err != nil {
		return nil, fmt.Errorf("event: decode %q: %w", eventType, err)
	}
	return v, nil
}

// CurrentVersion reports the registered current schema version for eventType
// (0 when unknown).
func (r *Registry) CurrentVersion(eventType string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentV[eventType]
}

// IsKnown reports whether eventType is registered.
func (r *Registry) IsKnown(eventType string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.currentV[eventType]
	return ok
}

// UnknownEventError means the binary has no handler for this event type at
// all (possibly produced by a newer deployment).
type UnknownEventError struct {
	EventType string
	Version   int
}

func (e *UnknownEventError) Error() string {
	return fmt.Sprintf("event: unknown event type %q (stored version %d)", e.EventType, e.Version)
}

// NewerSchemaError means the stored event revision is ahead of the binary.
type NewerSchemaError struct {
	EventType string
	Have      int
	Want      int
}

func (e *NewerSchemaError) Error() string {
	return fmt.Sprintf("event: event %q stored as v%d but this binary only understands up to v%d", e.EventType, e.Have, e.Want)
}

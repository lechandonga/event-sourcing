package eventstore

import (
	"errors"
	"fmt"
)

// ConflictReason explains *why* an append was rejected so callers can react
// differently (re-fetch and retry vs. drop vs. surface a user error).
type ConflictReason string

const (
	// ConflictStreamExists: caller expected a new stream (expectedVersion=0)
	// but the stream already contains committed events.
	ConflictStreamExists ConflictReason = "stream_exists"
	// ConflictStreamNotFound: caller expected an existing stream
	// (expectedVersion>0) but no stream exists.
	ConflictStreamNotFound ConflictReason = "stream_not_found"
	// ConflictVersionMismatch: the caller based the write on an outdated
	// version; another commit advanced the stream in between.
	ConflictVersionMismatch ConflictReason = "version_mismatch"
	// ConflictEmptyAppend: the events slice was empty.
	ConflictEmptyAppend ConflictReason = "empty_append"
	// ConflictInvalidEvent: a single event in the batch was malformed
	// (missing type/schema/payload...).
	ConflictInvalidEvent ConflictReason = "invalid_event"
	// ConflictDuplicateEventID: the event id is already present in the
	// committed history. The event is NOT appended again.
	ConflictDuplicateEventID ConflictReason = "duplicate_event_id"
)

// ConflictError is returned for every append that cannot take effect under
// optimistic concurrency rules. It is the single distinguishable conflict
// type; inspect Reason for the specific cause.
type ConflictError struct {
	Reason          ConflictReason
	AggregateType   string
	AggregateID     string
	ExpectedVersion int
	ActualVersion   int
	EventID         string
	Message         string
}

func (e *ConflictError) Error() string {
	s := fmt.Sprintf("eventstore: append conflict on %s/%s: %s", e.AggregateType, e.AggregateID, e.Reason)
	if e.Message != "" {
		s += ": " + e.Message
	}
	if e.ExpectedVersion != 0 || e.ActualVersion != 0 {
		s += fmt.Sprintf(" (expected version=%d, actual=%d)", e.ExpectedVersion, e.ActualVersion)
	}
	return s
}

// AsConflict extracts a *ConflictError from err, if any.
func AsConflict(err error) (*ConflictError, bool) {
	var ce *ConflictError
	if errors.As(err, &ce) {
		return ce, true
	}
	return nil, false
}

// IsConflict reports whether err is a *ConflictError.
func IsConflict(err error) bool {
	_, ok := AsConflict(err)
	return ok
}

// CorruptionError indicates that locally persisted data cannot be read.
type CorruptionError struct {
	File    string
	Offset  int64
	Message string
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("eventstore: corrupted local storage %s at offset %d: %s", e.File, e.Offset, e.Message)
}

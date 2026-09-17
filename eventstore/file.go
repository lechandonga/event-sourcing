package eventstore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lechandonga/event-sourcing/internal/safeio"
)

// DefaultReadBatchSize bounds a single ReadAll call against the file store.
const DefaultReadBatchSize = 500

const logFileName = "events.log"

// diskRecord is the on-disk envelope: the canonical record plus a CRC32 of
// the canonical record bytes. A mismatch (torn write / bit rot / tampering)
// is reported as a CorruptionError carrying the byte offset of the bad line.
type diskRecord struct {
	R   json.RawMessage `json:"r"`
	CRC uint32          `json:"crc"`
}

// FileEventStore is a durable, fully local event store. Events live in a
// single append-only JSON-lines file (events.log). Per-stream indexes are
// rebuilt from the log when the store is opened, so no separate index file
// can ever drift from the committed history.
//
// Appends are serialized by an in-process mutex; each batch is followed by
// an fsync before success is reported.
type FileEventStore struct {
	dir string

	mu       sync.Mutex
	f        *os.File
	streams  map[streamKey][]Envelope
	all      []Envelope
	position int64
	ids      map[string]struct{}
}

// OpenFileEventStore opens (creating if needed) a durable store in dir.
// The whole committed log is validated on open:
//   - CRC of every line must match;
//   - global positions must be 1..N gap-free;
//   - each aggregate stream's versions must be 1..M gap-free;
//   - event ids must be unique.
func OpenFileEventStore(dir string) (*FileEventStore, error) {
	if err := safeio.EnsureDir(dir); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, logFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("eventstore: open %s: %w", path, err)
	}

	s := &FileEventStore{
		dir:     dir,
		f:       f,
		streams: make(map[streamKey][]Envelope),
		ids:     make(map[string]struct{}),
	}
	if err := s.replay(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

// Dir reports the storage directory.
func (s *FileEventStore) Dir() string { return s.dir }

// Close releases the underlying file.
func (s *FileEventStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

func (s *FileEventStore) replay() error {
	path := filepath.Join(s.dir, logFileName)
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("eventstore: open %s for replay: %w", path, err)
	}
	defer f.Close()

	br := bufio.NewReader(f)
	var offset int64
	lineNo := 0
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			lineNo++
			lineStart := offset
			offset += int64(len(line))
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) == 0 {
				if errors.Is(err, io.EOF) {
					break
				}
				continue
			}
			if en, perr := parseLine(trimmed); perr != nil {
				return &CorruptionError{File: path, Offset: lineStart, Message: fmt.Sprintf("line %d: %v", lineNo, perr)}
			} else if ierr := s.indexCommitted(en); ierr != nil {
				return &CorruptionError{File: path, Offset: lineStart, Message: fmt.Sprintf("line %d: %v", lineNo, ierr)}
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("eventstore: read %s: %w", path, err)
		}
	}
	return nil
}

func parseLine(line []byte) (Envelope, error) {
	var dr diskRecord
	if err := json.Unmarshal(line, &dr); err != nil {
		return Envelope{}, fmt.Errorf("invalid json record: %w", err)
	}
	if crc32.ChecksumIEEE(dr.R) != dr.CRC {
		return Envelope{}, errors.New("crc mismatch")
	}
	var en Envelope
	if err := json.Unmarshal(dr.R, &en); err != nil {
		return Envelope{}, fmt.Errorf("invalid envelope: %w", err)
	}
	if en.EventType == "" || en.SchemaVersion < 1 || en.Version < 1 || en.Position < 1 {
		return Envelope{}, errors.New("record missing mandatory fields")
	}
	if len(en.Payload) == 0 || !json.Valid(en.Payload) {
		return Envelope{}, errors.New("record payload missing/invalid")
	}
	return en, nil
}

// indexCommitted validates ordering invariants and adds the envelope to the
// in-memory indexes. Called under no lock during initial replay and under
// s.mu for freshly appended events.
func (s *FileEventStore) indexCommitted(en Envelope) error {
	if en.Position != s.position+1 {
		return fmt.Errorf("global position gap: expected %d, record has %d", s.position+1, en.Position)
	}
	key := streamKey{en.AggregateType, en.AggregateID}
	if en.Version != len(s.streams[key])+1 {
		return fmt.Errorf("stream %s/%s version gap: expected %d, record has %d",
			en.AggregateType, en.AggregateID, len(s.streams[key])+1, en.Version)
	}
	if en.EventID == "" {
		return errors.New("empty event id")
	}
	if _, dup := s.ids[en.EventID]; dup {
		return fmt.Errorf("duplicate event id %q", en.EventID)
	}
	s.ids[en.EventID] = struct{}{}
	s.position = en.Position
	s.streams[key] = append(s.streams[key], en)
	s.all = append(s.all, en)
	return nil
}

// Append implements EventStore.
func (s *FileEventStore) Append(ctx context.Context, aggregateType, aggregateID string, expectedVersion int, events []UncommittedEvent) ([]Envelope, error) {
	if ce, id := validateBatch(events); ce != nil {
		ce.AggregateType = aggregateType
		ce.AggregateID = aggregateID
		ce.ExpectedVersion = expectedVersion
		ce.EventID = id
		return nil, ce
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.f == nil {
		return nil, errors.New("eventstore: store is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	key := streamKey{aggregateType, aggregateID}
	current := s.streams[key]
	actual := len(current)

	switch {
	case expectedVersion == 0 && actual != 0:
		return nil, &ConflictError{
			Reason: ConflictStreamExists, AggregateType: aggregateType, AggregateID: aggregateID,
			ExpectedVersion: 0, ActualVersion: actual,
			Message: "stream already exists; load it and append at its current version",
		}
	case expectedVersion > 0 && actual == 0:
		return nil, &ConflictError{
			Reason: ConflictStreamNotFound, AggregateType: aggregateType, AggregateID: aggregateID,
			ExpectedVersion: expectedVersion, ActualVersion: 0,
			Message: "stream does not exist",
		}
	case expectedVersion > 0 && actual != expectedVersion:
		return nil, &ConflictError{
			Reason: ConflictVersionMismatch, AggregateType: aggregateType, AggregateID: aggregateID,
			ExpectedVersion: expectedVersion, ActualVersion: actual,
			Message: "stream was modified concurrently; reload and retry",
		}
	}

	for i := range events {
		id := events[i].EventID
		if id == "" {
			return nil, &ConflictError{
				Reason: ConflictInvalidEvent, AggregateType: aggregateType, AggregateID: aggregateID,
				ExpectedVersion: expectedVersion, ActualVersion: actual,
				Message: "event id is empty",
			}
		}
		if _, dup := s.ids[id]; dup {
			return nil, &ConflictError{
				Reason: ConflictDuplicateEventID, AggregateType: aggregateType, AggregateID: aggregateID,
				ExpectedVersion: expectedVersion, ActualVersion: actual, EventID: id,
				Message: "event with this id is already committed",
			}
		}
	}
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

	// Encode the whole batch and assign tentative positions/versions BEFORE
	// touching any state, so an encoding error leaves the store untouched.
	now := time.Now().UTC()
	committed := make([]Envelope, 0, len(events))
	encoded := make([][]byte, 0, len(events))
	nextVersion := actual
	nextPosition := s.position
	for i := range events {
		ev := &events[i]
		nextVersion++
		nextPosition++
		ts := ev.Timestamp
		if ts.IsZero() {
			ts = now
		}
		en := Envelope{
			EventID:       ev.EventID,
			AggregateType: aggregateType,
			AggregateID:   aggregateID,
			Version:       nextVersion,
			Position:      nextPosition,
			EventType:     ev.EventType,
			SchemaVersion: ev.SchemaVersion,
			Timestamp:     ts,
			Payload:       append(json.RawMessage(nil), ev.Payload...),
			Metadata:      cloneMetadata(ev.Metadata),
		}
		line, err := encodeLine(en)
		if err != nil {
			return nil, fmt.Errorf("eventstore: encode event: %w", err)
		}
		committed = append(committed, en)
		encoded = append(encoded, line)
	}

	// One write + one fsync per batch.
	var buf bytes.Buffer
	for _, line := range encoded {
		buf.Write(line)
	}
	if _, err := s.f.Write(buf.Bytes()); err != nil {
		// Indexes were not updated. Reopening the log reconciles truth from
		// disk (the partially written trailing line fails CRC and is reported
		// explicitly rather than being silently accepted).
		return nil, fmt.Errorf("eventstore: write log: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		return nil, fmt.Errorf("eventstore: fsync log: %w", err)
	}
	for i := range committed {
		if err := s.indexCommitted(committed[i]); err != nil {
			// Cannot happen: ordering was computed above; surface loudly.
			return nil, fmt.Errorf("eventstore: internal index error: %w", err)
		}
	}
	return committed, nil
}

func encodeLine(en Envelope) ([]byte, error) {
	r, err := json.Marshal(en)
	if err != nil {
		return nil, err
	}
	line, err := json.Marshal(diskRecord{R: r, CRC: crc32.ChecksumIEEE(r)})
	if err != nil {
		return nil, err
	}
	line = append(line, '\n')
	return line, nil
}

// ReadStream implements EventStore.
func (s *FileEventStore) ReadStream(_ context.Context, aggregateType, aggregateID string, opts ReadOptions) ([]Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
func (s *FileEventStore) ReadAll(_ context.Context, fromPosition int64, maxEvents int) ([]Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if maxEvents <= 0 {
		maxEvents = DefaultReadBatchSize
	}
	var out []Envelope
	for _, e := range s.all {
		if e.Position <= fromPosition {
			continue
		}
		if len(out) >= maxEvents {
			break
		}
		out = append(out, e)
	}
	return cloneEnvelopes(out), nil
}

// StreamVersion implements EventStore.
func (s *FileEventStore) StreamVersion(_ context.Context, aggregateType, aggregateID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams[streamKey{aggregateType, aggregateID}]), nil
}

// LastPosition implements EventStore.
func (s *FileEventStore) LastPosition(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.position, nil
}

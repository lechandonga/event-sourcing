package eventstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/lechandonga/event-sourcing/eventstore"
)

func mkEvent(id string) eventstore.UncommittedEvent {
	return eventstore.UncommittedEvent{
		EventID:       id,
		EventType:     "test.e",
		SchemaVersion: 1,
		Payload:       json.RawMessage(`{"x":1}`),
	}
}

func stores(t *testing.T) map[string]eventstore.EventStore {
	t.Helper()
	out := map[string]eventstore.EventStore{
		"memory": eventstore.NewMemoryEventStore(),
	}
	dir := t.TempDir()
	fs, err := eventstore.OpenFileEventStore(dir)
	if err != nil {
		t.Fatalf("open file store: %v", err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	out["file"] = fs
	return out
}

func TestAppendAndReadOrder(t *testing.T) {
	for name, st := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			evs := []eventstore.UncommittedEvent{mkEvent("e1"), mkEvent("e2")}
			got, err := st.Append(ctx, "agg", "a1", 0, evs)
			if err != nil {
				t.Fatalf("append: %v", err)
			}
			if got[0].Version != 1 || got[1].Version != 2 {
				t.Fatalf("versions = %d,%d", got[0].Version, got[1].Version)
			}
			if got[0].Position != 1 || got[1].Position != 2 {
				t.Fatalf("positions = %d,%d", got[0].Position, got[1].Position)
			}
			stream, err := st.ReadStream(ctx, "agg", "a1", eventstore.ReadOptions{})
			if err != nil || len(stream) != 2 {
				t.Fatalf("readstream: %v %d", err, len(stream))
			}
			all, _ := st.ReadAll(ctx, 0, 0)
			if len(all) != 2 || all[1].EventID != "e2" {
				t.Fatalf("readall order wrong: %+v", all)
			}
		})
	}
}

func TestConflictReasonsAreDistinguishable(t *testing.T) {
	for name, st := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if _, err := st.Append(ctx, "agg", "a1", 0, []eventstore.UncommittedEvent{mkEvent("e1")}); err != nil {
				t.Fatal(err)
			}

			// expectedVersion=0 but stream exists.
			_, err := st.Append(ctx, "agg", "a1", 0, []eventstore.UncommittedEvent{mkEvent("e2")})
			ce, ok := eventstore.AsConflict(err)
			if !ok || ce.Reason != eventstore.ConflictStreamExists {
				t.Fatalf("want stream_exists, got %v", err)
			}

			// outdated expected version (stream is currently at v1).
			_, err = st.Append(ctx, "agg", "a1", 99, []eventstore.UncommittedEvent{mkEvent("e3")})
			if ce, ok = eventstore.AsConflict(err); !ok || ce.Reason != eventstore.ConflictVersionMismatch ||
				ce.ExpectedVersion != 99 || ce.ActualVersion != 1 {
				t.Fatalf("want version_mismatch, got %v", err)
			}

			// stream not found.
			_, err = st.Append(ctx, "agg", "missing", 5, []eventstore.UncommittedEvent{mkEvent("e4")})
			if ce, ok = eventstore.AsConflict(err); !ok || ce.Reason != eventstore.ConflictStreamNotFound {
				t.Fatalf("want stream_not_found, got %v", err)
			}

			// duplicate event id.
			_, err = st.Append(ctx, "agg", "a2", 0, []eventstore.UncommittedEvent{mkEvent("e1")})
			if ce, ok = eventstore.AsConflict(err); !ok || ce.Reason != eventstore.ConflictDuplicateEventID {
				t.Fatalf("want duplicate_event_id, got %v", err)
			}

			// empty batch.
			_, err = st.Append(ctx, "agg", "a3", 0, nil)
			if ce, ok = eventstore.AsConflict(err); !ok || ce.Reason != eventstore.ConflictEmptyAppend {
				t.Fatalf("want empty_append, got %v", err)
			}

			// rejected writes must not enter the committed stream.
			v, _ := st.StreamVersion(ctx, "agg", "a1")
			if v != 1 {
				t.Fatalf("conflicting writes leaked into stream, version=%d", v)
			}
			all, _ := st.ReadAll(ctx, 0, 0)
			if len(all) != 1 {
				t.Fatalf("committed history changed after rejected appends: %d events", len(all))
			}
		})
	}
}

func TestConcurrentSameAggregateExactlyOneWinsPerVersion(t *testing.T) {
	for name, st := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			const writers = 32
			var wg sync.WaitGroup
			var okN, conflictN int64
			var mu sync.Mutex
			wg.Add(writers)
			for i := 0; i < writers; i++ {
				i := i
				go func() {
					defer wg.Done()
					id := fmt.Sprintf("ce-%d", i)
					_, err := st.Append(ctx, "agg", "hot", 0, []eventstore.UncommittedEvent{mkEvent(id)})
					mu.Lock()
					defer mu.Unlock()
					if err == nil {
						okN++
						return
					}
					if eventstore.IsConflict(err) {
						conflictN++
						return
					}
					t.Errorf("unexpected error: %v", err)
				}()
			}
			wg.Wait()
			if okN != 1 || conflictN != writers-1 {
				t.Fatalf("ok=%d conflicts=%d", okN, conflictN)
			}
			v, _ := st.StreamVersion(ctx, "agg", "hot")
			if v != 1 {
				t.Fatalf("stream version = %d, want 1", v)
			}
		})
	}
}

func TestConcurrentVersionedAppendsSerialize(t *testing.T) {
	// Each writer loads the current version, then appends; retry on conflict.
	// Final stream must be a gap-free 1..N sequence.
	for name, st := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			const n = 40
			var wg sync.WaitGroup
			errs := make(chan error, n)
			wg.Add(n)
			for i := 0; i < n; i++ {
				i := i
				go func() {
					defer wg.Done()
					for attempt := 0; attempt < 50; attempt++ {
						v, err := st.StreamVersion(ctx, "agg", "seq")
						if err != nil {
							errs <- err
							return
						}
						_, err = st.Append(ctx, "agg", "seq", v,
							[]eventstore.UncommittedEvent{mkEvent(fmt.Sprintf("seq-%d-%d", i, attempt))})
						if err == nil {
							return
						}
						if !eventstore.IsConflict(err) {
							errs <- err
							return
						}
					}
					errs <- errors.New("writer gave up")
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			got, _ := st.ReadStream(ctx, "agg", "seq", eventstore.ReadOptions{})
			if len(got) != n {
				t.Fatalf("len=%d want %d", len(got), n)
			}
			for i, e := range got {
				if e.Version != i+1 {
					t.Fatalf("gap/order at %d: version=%d", i, e.Version)
				}
			}
		})
	}
}

func TestGlobalPositionIsGapFreeAcrossStreams(t *testing.T) {
	st := eventstore.NewMemoryEventStore()
	ctx := context.Background()
	var wg sync.WaitGroup
	for a := 0; a < 10; a++ {
		a := a
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("a%d", a)
			for e := 0; e < 20; e++ {
				v, _ := st.StreamVersion(ctx, "agg", id)
				_, err := st.Append(ctx, "agg", id, v,
					[]eventstore.UncommittedEvent{mkEvent(fmt.Sprintf("g-%d-%d", a, e))})
				if err != nil && !eventstore.IsConflict(err) {
					t.Errorf("append: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	all, _ := st.ReadAll(ctx, 0, 0)
	for i := range all {
		if all[i].Position != int64(i+1) {
			t.Fatalf("position gap at %d: %d", i, all[i].Position)
		}
	}
}

func TestInvalidBatchRejected(t *testing.T) {
	st := eventstore.NewMemoryEventStore()
	ctx := context.Background()
	bad := eventstore.UncommittedEvent{EventID: "x", EventType: "", SchemaVersion: 1, Payload: json.RawMessage(`{}`)}
	_, err := st.Append(ctx, "agg", "a", 0, []eventstore.UncommittedEvent{bad})
	ce, ok := eventstore.AsConflict(err)
	if !ok || ce.Reason != eventstore.ConflictInvalidEvent {
		t.Fatalf("want invalid_event, got %v", err)
	}
}

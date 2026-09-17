package eventstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/lechandonga/event-sourcing/eventstore"
)

func TestFileStoreDurabilityAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	func() {
		st, err := eventstore.OpenFileEventStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		if _, err := st.Append(ctx, "agg", "a1", 0, []eventstore.UncommittedEvent{mkEvent("d1")}); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Append(ctx, "agg", "a1", 1, []eventstore.UncommittedEvent{mkEvent("d2")}); err != nil {
			t.Fatal(err)
		}
	}()

	st2, err := eventstore.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	v, _ := st2.StreamVersion(ctx, "agg", "a1")
	if v != 2 {
		t.Fatalf("version after reopen = %d", v)
	}
	pos, _ := st2.LastPosition(ctx)
	if pos != 2 {
		t.Fatalf("position after reopen = %d", pos)
	}
}

func TestFileStoreReopenRejectsCorruptedTrailingLine(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	st, err := eventstore.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(ctx, "agg", "a1", 0, []eventstore.UncommittedEvent{mkEvent("d1")}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "events.log")
	// Flip a byte inside the payload to break CRC.
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	idx := strings.Index(string(data), `"x":1`)
	data[idx+4] = '2'
	if err := os.WriteFile(logPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = eventstore.OpenFileEventStore(dir)
	var ce *eventstore.CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("want CorruptionError, got %v", err)
	}
	if ce.Offset != 0 {
		t.Fatalf("corruption offset = %d, want 0", ce.Offset)
	}
}

func TestFileStoreConcurrentWritersAndIndexes(t *testing.T) {
	dir := t.TempDir()
	st, err := eventstore.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	const writers, rounds = 8, 25
	var wg sync.WaitGroup
	for a := 0; a < writers; a++ {
		a := a
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := string(rune('a' + a))
			for i := 0; i < rounds; i++ {
				for {
					v, _ := st.StreamVersion(ctx, "agg", id)
					ev := mkEvent("fc-" + id + "-" + string(rune('0'+i%10)) + "-" + itoa(v))
					_, err := st.Append(ctx, "agg", id, v, []eventstore.UncommittedEvent{ev})
					if err == nil {
						break
					}
					if !eventstore.IsConflict(err) {
						t.Errorf("append: %v", err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	st.Close()

	// Reopen and verify stream + global invariants.
	st2, err := eventstore.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	all, _ := st2.ReadAll(ctx, 0, 0)
	if len(all) != writers*rounds {
		t.Fatalf("total=%d want %d", len(all), writers*rounds)
	}
	for i := range all {
		if all[i].Position != int64(i+1) {
			t.Fatalf("global gap %d -> %d", i, all[i].Position)
		}
	}
	for a := 0; a < writers; a++ {
		id := string(rune('a' + a))
		evs, _ := st2.ReadStream(ctx, "agg", id, eventstore.ReadOptions{})
		if len(evs) != rounds {
			t.Fatalf("stream %s len=%d", id, len(evs))
		}
		for i, e := range evs {
			if e.Version != i+1 {
				t.Fatalf("stream %s version gap: %d", id, e.Version)
			}
		}
	}
}

func TestFileStorePayloadBytesNeverChangedByReads(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	st, err := eventstore.OpenFileEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	raw := json.RawMessage(`{"old_field":"kept","x":1}`)
	ev := eventstore.UncommittedEvent{EventID: "p1", EventType: "t", SchemaVersion: 1, Payload: raw}
	if _, err := st.Append(ctx, "agg", "a", 0, []eventstore.UncommittedEvent{ev}); err != nil {
		t.Fatal(err)
	}
	got, _ := st.ReadAll(ctx, 0, 0)
	if string(got[0].Payload) != string(raw) {
		t.Fatalf("payload mutated: %s", got[0].Payload)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "events.log"))
	if !strings.Contains(string(data), `"old_field":"kept"`) {
		t.Fatalf("stored bytes lost original field: %s", data)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

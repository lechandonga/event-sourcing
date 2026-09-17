package snapshot_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/lechandonga/event-sourcing/snapshot"
)

func stores(t *testing.T) map[string]snapshot.Store {
	t.Helper()
	return map[string]snapshot.Store{
		"memory": snapshot.NewMemoryStore(),
		"file":   snapshot.NewFileStore(t.TempDir()),
	}
}

func sample() *snapshot.Snapshot {
	return &snapshot.Snapshot{
		AggregateType: "agg", AggregateID: "a1", Version: 7,
		SchemaVersion: 1, TakenAt: time.Now().UTC(), State: []byte(`{"v":1}`),
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	for name, st := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if err := st.Save(ctx, sample()); err != nil {
				t.Fatal(err)
			}
			got, err := st.Load(ctx, "agg", "a1")
			if err != nil {
				t.Fatal(err)
			}
			var state map[string]int
			if err := json.Unmarshal(got.State, &state); err != nil || state["v"] != 1 {
				t.Fatalf("state round trip wrong: %s err=%v", got.State, err)
			}
			if got.Version != 7 || got.Checksum == 0 {
				t.Fatalf("round trip wrong: %+v", got)
			}
		})
	}
}

func TestMissingSnapshotIsDistinguishable(t *testing.T) {
	for name, st := range stores(t) {
		t.Run(name, func(t *testing.T) {
			_, err := st.Load(context.Background(), "agg", "nope")
			var nf *snapshot.ErrSnapshotNotFound
			if !errors.As(err, &nf) {
				t.Fatalf("want ErrSnapshotNotFound, got %v", err)
			}
		})
	}
}

func TestCorruptedSnapshotFailsChecksum(t *testing.T) {
	ctx := context.Background()

	mem := snapshot.NewMemoryStore()
	if err := mem.Save(ctx, sample()); err != nil {
		t.Fatal(err)
	}
	mem.CorruptTest("agg", "a1", []byte(`{"v":999}`))
	if _, err := mem.Load(ctx, "agg", "a1"); !isCorruption(err) {
		t.Fatalf("memory: want CorruptionError, got %v", err)
	}

	dir := t.TempDir()
	fs := snapshot.NewFileStore(dir)
	if err := fs.Save(ctx, sample()); err != nil {
		t.Fatal(err)
	}
	if err := fs.CorruptTest("agg", "a1"); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Load(ctx, "agg", "a1"); !isCorruption(err) {
		t.Fatalf("file: want CorruptionError, got %v", err)
	}
}

func TestSnapshotReplaceSemantics(t *testing.T) {
	fs := snapshot.NewFileStore(t.TempDir())
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		s := sample()
		s.Version = i + 1
		if err := fs.Save(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	got, err := fs.Load(ctx, "agg", "a1")
	if err != nil || got.Version != 5 {
		t.Fatalf("replace semantics wrong: %+v err=%v", got, err)
	}
}

func isCorruption(err error) bool {
	var ce *snapshot.CorruptionError
	return errors.As(err, &ce)
}

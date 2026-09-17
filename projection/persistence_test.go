package projection_test

import (
	"context"
	"testing"

	"github.com/lechandonga/event-sourcing/projection"
)

func TestFileCheckpointPersistenceAndMonotonic(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cs := projection.NewFileCheckpointStore(dir)
	if err := cs.Save(ctx, projection.Checkpoint{Projection: "p", Sequence: 5}); err != nil {
		t.Fatal(err)
	}
	if err := cs.Save(ctx, projection.Checkpoint{Projection: "p", Sequence: 3}); err == nil {
		t.Fatal("checkpoint regression must be rejected")
	}
	if err := cs.Reset(ctx, "p"); err != nil {
		t.Fatal(err)
	}
	cp, err := cs.Load(ctx, "p")
	if err != nil {
		t.Fatal(err)
	}
	if cp.Sequence != 0 {
		t.Fatalf("after reset seq=%d want 0", cp.Sequence)
	}
	// 从零重新前进应被允许
	if err := cs.Save(ctx, projection.Checkpoint{Projection: "p", Sequence: 1}); err != nil {
		t.Fatalf("save after reset: %v", err)
	}
	// 不存在的检查点读为零值
	cp2, err := cs.Load(ctx, "other")
	if err != nil || cp2.Sequence != 0 {
		t.Fatalf("missing checkpoint should read zero: %+v err=%v", cp2, err)
	}
}

func TestFileFailureStoreLifecycle(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs := projection.NewFileFailureStore(dir)
	if f, err := fs.Load(ctx, "p"); err != nil || f != nil {
		t.Fatalf("empty store: f=%v err=%v", f, err)
	}
	rec := &projection.Failure{
		Projection: "p", Sequence: 9, Stream: "s-1", StreamVersion: 3,
		EventType: "E", EventID: "id-9",
		Message: "boom", FirstOccurredAt: "t1", LastOccurredAt: "t1",
	}
	if err := fs.Save(ctx, rec); err != nil {
		t.Fatal(err)
	}
	got, err := fs.Load(ctx, "p")
	if err != nil || got == nil || got.Sequence != 9 {
		t.Fatalf("load failure: %+v err=%v", got, err)
	}
	// 重试同一事件：Attempts 累加
	rec2 := *rec
	rec2.LastOccurredAt = "t2"
	if err := fs.Save(ctx, &rec2); err != nil {
		t.Fatal(err)
	}
	got, _ = fs.Load(ctx, "p")
	if got.Attempts != 2 {
		t.Fatalf("attempts=%d want 2", got.Attempts)
	}
	if err := fs.Clear(ctx, "p"); err != nil {
		t.Fatal(err)
	}
	if f, _ := fs.Load(ctx, "p"); f != nil {
		t.Fatalf("after clear should be nil, got %+v", f)
	}
}

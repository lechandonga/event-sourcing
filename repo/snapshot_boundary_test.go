package repo_test

import (
	"context"
	"testing"

	"github.com/lechandonga/event-sourcing/bank"
	"github.com/lechandonga/event-sourcing/eventstore"
	"github.com/lechandonga/event-sourcing/snapshot"
)

// 阈值为 10 时，前 9 个版本不打快照；第 10 个版本必须打且快照版本必须等于 10。
func TestSnapshotThresholdNotTriggeredPrematurely(t *testing.T) {
	ctx := context.Background()
	snaps := snapshot.NewMemoryStore()
	r, _ := newFixture(t, eventstore.NewMemoryStore(), snaps, 10, nil)
	openAccount(t, ctx, r, "a1", 0) // v1
	got, ver, err := r.Load(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if ver != 1 || got.Version() != 1 {
		t.Fatalf("setup wrong: ver=%d", ver)
	}
	if _, err := snaps.Load(ctx, bank.AccountType, "a1"); err == nil {
		t.Fatal("no snapshot should exist before threshold")
	}
}

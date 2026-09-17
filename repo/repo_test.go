package repo_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/event-sourcing/bank"
	"github.com/lechandonga/event-sourcing/eventstore"
	"github.com/lechandonga/event-sourcing/repo"
	"github.com/lechandonga/event-sourcing/snapshot"
)

func newRepo(t *testing.T, st eventstore.EventStore, snaps snapshot.Store, opts repo.Options) *repo.Repository {
	t.Helper()
	reg := bank.NewRegistry()
	return repo.New(st, snaps, reg, func(id string) repo.Aggregate { return bank.NewAccount(id) }, opts)
}

func openAccount(t *testing.T, ctx context.Context, r *repo.Repository, id string) {
	t.Helper()
	for {
		agg, _, err := r.Load(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		a := agg.(*bank.Account)
		if a.Version() > 0 {
			return // already open
		}
		if err := a.Open("Ada", "USD", "checking", 0); err != nil {
			t.Fatal(err)
		}
		if _, _, err := r.Save(ctx, a); err == nil {
			return
		} else if eventstore.IsConflict(err) {
			continue
		} else {
			t.Fatal(err)
		}
	}
}

func deposit(t *testing.T, ctx context.Context, r *repo.Repository, id string, amount int64) {
	t.Helper()
	openAccount(t, ctx, r, id)
	for {
		agg, _, err := r.Load(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if err := agg.(*bank.Account).Deposit(amount, "", "wire", ""); err != nil {
			t.Fatal(err)
		}
		if _, _, err := r.Save(ctx, agg); err == nil {
			return
		} else if eventstore.IsConflict(err) {
			continue
		} else {
			t.Fatal(err)
		}
	}
}

func TestSnapshotAcceleratedLoadMatchesFullReplay(t *testing.T) {
	st := eventstore.NewMemoryEventStore()
	snaps := snapshot.NewMemoryStore()
	r := newRepo(t, st, snaps, repo.Options{SnapshotInterval: 5})
	ctx := context.Background()
	for i := 0; i < 12; i++ {
		deposit(t, ctx, r, "a1", 10)
	}
	// 1 open + 12 deposits => v13; latest snapshot at v10, tail of 3.

	// Snapshot at v10 should exist; load should replay only the tail.
	agg, info, err := r.Load(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode != "snapshot+replay" {
		t.Fatalf("load mode=%s", info.Mode)
	}
	if info.SnapshotVersion != 10 || info.ReplayedCount != 3 {
		t.Fatalf("snapshot=%d replayed=%d", info.SnapshotVersion, info.ReplayedCount)
	}
	a := agg.(*bank.Account)
	if a.Balance() != 120 || a.Version() != 13 {
		t.Fatalf("balance=%d version=%d", a.Balance(), a.Version())
	}

	// Fresh repository with snapshot store wiped: full replay converges.
	r2 := newRepo(t, st, snapshot.NewMemoryStore(), repo.Options{SnapshotInterval: 5})
	agg2, info2, err := r2.Load(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if info2.Mode != "full_replay" || info2.ReplayedCount != 13 {
		t.Fatalf("full replay mode=%s count=%d", info2.Mode, info2.ReplayedCount)
	}
	if agg2.(*bank.Account).Balance() != 120 {
		t.Fatalf("full replay balance=%d", agg2.(*bank.Account).Balance())
	}
}

func TestCorruptedSnapshotFallsBackToFullReplay(t *testing.T) {
	st := eventstore.NewMemoryEventStore()
	snaps := snapshot.NewMemoryStore()
	var warned int64
	r := newRepo(t, st, snaps, repo.Options{
		SnapshotInterval: 3,
		OnSnapshotWarning: func(_, _ string, _ error) {
			_ = atomicAdd(&warned)
		},
	})
	ctx := context.Background()
	for i := 0; i < 8; i++ {
		deposit(t, ctx, r, "a1", 10)
	}
	// 1 open + 8 deposits = v9
	snaps.CorruptTest(bank.AggregateType, "a1", []byte(`{"balance":999999}`))

	agg, info, err := r.Load(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode != "full_replay" || info.ReplayedCount != 9 {
		t.Fatalf("mode=%s count=%d", info.Mode, info.ReplayedCount)
	}
	if agg.(*bank.Account).Balance() != 80 {
		t.Fatalf("corrupted state leaked: balance=%d", agg.(*bank.Account).Balance())
	}
	if warned == 0 {
		t.Fatal("expected a snapshot warning callback")
	}
}

func TestStaleSnapshotByAgeFallsBack(t *testing.T) {
	st := eventstore.NewMemoryEventStore()
	snaps := snapshot.NewMemoryStore()
	r := newRepo(t, st, snaps, repo.Options{SnapshotInterval: 2, MaxSnapshotAge: time.Hour})
	ctx := context.Background()
	deposit(t, ctx, r, "a1", 10)
	deposit(t, ctx, r, "a1", 10) // snapshot at v2

	// A repository that considers anything older than 1ns stale.
	rStrict := repo.New(st, snaps, bank.NewRegistry(),
		func(id string) repo.Aggregate { return bank.NewAccount(id) },
		repo.Options{SnapshotInterval: 2, MaxSnapshotAge: 1})
	agg, info, err := rStrict.Load(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode != "full_replay" {
		t.Fatalf("stale snapshot must force full replay, mode=%s", info.Mode)
	}
	if agg.(*bank.Account).Balance() != 20 {
		t.Fatalf("balance=%d", agg.(*bank.Account).Balance())
	}
}

func TestMissingSnapshotReplaysFromOne(t *testing.T) {
	st := eventstore.NewMemoryEventStore()
	snaps := snapshot.NewMemoryStore()
	r := newRepo(t, st, snaps, repo.Options{SnapshotInterval: 100}) // never snapshots
	ctx := context.Background()
	for i := 0; i < 7; i++ {
		deposit(t, ctx, r, "a1", 5)
	}
	agg, info, err := r.Load(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode != "full_replay" || info.ReplayedCount != 8 {
		t.Fatalf("mode=%s count=%d", info.Mode, info.ReplayedCount)
	}
	if agg.(*bank.Account).Balance() != 35 {
		t.Fatalf("balance=%d", agg.(*bank.Account).Balance())
	}
}

func TestSaveConflictIsRejectableAndRebaseSucceeds(t *testing.T) {
	st := eventstore.NewMemoryEventStore()
	snaps := snapshot.NewMemoryStore()
	r := newRepo(t, st, snaps, repo.Options{SnapshotInterval: 100})
	ctx := context.Background()
	deposit(t, ctx, r, "a1", 10) // v1..v2

	stale, _, err := r.Load(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	// concurrent writer
	deposit(t, ctx, r, "a1", 20) // v3

	if err := stale.(*bank.Account).Deposit(5, "", "wire", ""); err != nil {
		t.Fatal(err)
	}
	_, _, err = r.Save(ctx, stale)
	var ce *eventstore.ConflictError
	if !errors.As(err, &ce) || ce.Reason != eventstore.ConflictVersionMismatch {
		t.Fatalf("want version mismatch, got %v", err)
	}
	// stale event never entered the stream
	v, _ := st.StreamVersion(ctx, bank.AggregateType, "a1")
	if v != 3 {
		t.Fatalf("version=%d want 3", v)
	}

	// Rebase: reload fresh, apply decision again, save at current version.
	fresh, _, err := r.Load(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if err := fresh.(*bank.Account).Deposit(5, "rebased", "wire", ""); err != nil {
		t.Fatal(err)
	}
	_, _, err = r.Save(ctx, fresh)
	if err != nil {
		t.Fatalf("rebased save failed: %v", err)
	}
	final, _, _ := r.Load(ctx, "a1")
	if final.(*bank.Account).Balance() != 35 {
		t.Fatalf("balance=%d want 35", final.(*bank.Account).Balance())
	}
}

func TestConcurrentAggregateSavesWithRetry(t *testing.T) {
	st := eventstore.NewMemoryEventStore()
	snaps := snapshot.NewMemoryStore()
	r := newRepo(t, st, snaps, repo.Options{SnapshotInterval: 100})
	ctx := context.Background()
	deposit(t, ctx, r, "hot", 100) // open + deposit

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attempt := 0; attempt < 50; attempt++ {
				agg, _, err := r.Load(ctx, "hot")
				if err != nil {
					t.Errorf("load: %v", err)
					return
				}
				if err := agg.(*bank.Account).Deposit(1, fmt.Sprintf("a%d", attempt), "wire", ""); err != nil {
					t.Errorf("cmd: %v", err)
					return
				}
				if _, _, err := r.Save(ctx, agg); err == nil {
					return
				} else if !eventstore.IsConflict(err) {
					t.Errorf("save: %v", err)
					return
				}
			}
			t.Errorf("writer gave up")
		}()
	}
	wg.Wait()
	final, _, err := r.Load(ctx, "hot")
	if err != nil {
		t.Fatal(err)
	}
	// initial 100 + 20 ones
	if final.(*bank.Account).Balance() != 120 {
		t.Fatalf("balance=%d want 120", final.(*bank.Account).Balance())
	}
	if final.(*bank.Account).Version() != 22 {
		t.Fatalf("version=%d want 22", final.(*bank.Account).Version())
	}
}

// tiny helper to avoid importing sync/atomic for one counter
var counterMu sync.Mutex

func atomicAdd(p *int64) int64 {
	counterMu.Lock()
	defer counterMu.Unlock()
	*p++
	return *p
}

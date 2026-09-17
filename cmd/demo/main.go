// Command demo exercises the whole stack against a temp local directory:
//
//  1. opens a durable file event store + snapshot/checkpoint stores;
//  2. appends account events with optimistic concurrency (and shows one
//     rejected version conflict with its distinguishable reason);
//  3. plants a v1 historical event into the stream and shows it upcasting to
//     v2 during replay without modifying the stored bytes;
//  4. runs the incremental projection, rebuilds it from scratch and proves
//     both states are identical;
//  5. corrupts the snapshot and demonstrates safe full-replay fallback.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lechandonga/event-sourcing/bank"
	"github.com/lechandonga/event-sourcing/eventstore"
	"github.com/lechandonga/event-sourcing/projection"
	"github.com/lechandonga/event-sourcing/repo"
	"github.com/lechandonga/event-sourcing/snapshot"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "es-demo-")
	must(err)
	defer os.RemoveAll(dir)
	fmt.Println("local storage dir:", dir)

	store, err := eventstore.OpenFileEventStore(filepath.Join(dir, "store"))
	must(err)
	defer store.Close()
	snaps := snapshot.NewFileStore(dir)
	cps := projection.NewFileCheckpointStore(dir)
	mss := projection.NewFileStateStore(dir)
	registry := bank.NewRegistry()

	repos := repo.New(store, snaps, registry,
		func(id string) repo.Aggregate { return bank.NewAccount(id) }, repo.Options{
			SnapshotInterval: 3,
			OnSnapshotWarning: func(t, id string, err error) {
				fmt.Printf("[snapshot-warning] %s/%s: %v\n", t, id, err)
			},
		})

	// --- Aggregate writes -------------------------------------------------
	acc, _, err := repos.Load(ctx, "acct-1")
	must(err)
	must(acc.(*bank.Account).Open("Ada Lovelace", "USD", "checking", 500))
	_, _, err = repos.Save(ctx, acc)
	must(err)

	stale, _, err := repos.Load(ctx, "acct-1")
	must(err) // stale is at version 1

	acc2, _, err := repos.Load(ctx, "acct-1")
	must(err)
	must(acc2.(*bank.Account).Deposit(100, "salary", "wire", "tx-1"))
	_, _, err = repos.Save(ctx, acc2) // advances to v2
	must(err)

	// stale writer must be rejected with a clear reason.
	must(stale.(*bank.Account).Deposit(50, "stale", "branch", ""))
	_, _, err = repos.Save(ctx, stale)
	var ce *eventstore.ConflictError
	if errors.As(err, &ce) {
		fmt.Printf("conflict rejected: reason=%s expected=%d actual=%d\n",
			ce.Reason, ce.ExpectedVersion, ce.ActualVersion)
	} else {
		fail(fmt.Errorf("expected conflict, got %v", err))
	}

	// More ops to cross the snapshot interval boundary.
	acc3, _, err := repos.Load(ctx, "acct-1")
	must(err)
	must(acc3.(*bank.Account).Withdraw(30, "coffee", "card", "NORMAL"))
	_, _, err = repos.Save(ctx, acc3)
	must(err)
	acc5, _, err := repos.Load(ctx, "acct-1")
	must(err)
	must(acc5.(*bank.Account).Deposit(200, "bonus", "wire", "tx-2"))
	_, _, err = repos.Save(ctx, acc5)
	must(err)

	// A second account whose opening event is stored in the OLD v1 schema.
	must(plantV1Opened(store))

	// --- Incremental projection -------------------------------------------
	proj, err := projection.New("accounts", store, cps, bank.NewAccountsModel, projection.Options{
		Registry:          registry,
		PollInterval:      10 * time.Millisecond,
		StateStore:        mss,
		StateSaveInterval: 1,
	})
	must(err)
	runCtx, cancel := context.WithCancel(ctx)
	go func() { _ = proj.Run(runCtx) }()
	waitUntil(func() bool { return proj.Position() >= mustLast(store) })
	view1, _ := proj.Model().(*bank.AccountsModel).Account("acct-1")
	fmt.Printf("incremental: acct-1 balance=%d deposits=%d (position=%d)\n",
		view1.Balance, view1.DepositCount, proj.Position())

	// v1 account must have been upcast with stable defaults.
	old, ok := proj.Model().(*bank.AccountsModel).Account("acct-legacy")
	must2(ok)
	fmt.Printf("upcast v1->v2: type=%q creditLimit=%d\n", old.AccountType, old.CreditLimit)

	// --- Rebuild must converge --------------------------------------------
	cancel()
	must(proj.Rebuild(ctx))
	view2, _ := proj.Model().(*bank.AccountsModel).Account("acct-1")
	if !sameView(view1, view2) {
		fail(fmt.Errorf("rebuild mismatch:\n%+v\n%+v", view1, view2))
	}
	fmt.Printf("rebuild parity OK: balance=%d deposits=%d accounts=%d position=%d\n",
		view2.Balance, view2.DepositCount, proj.Model().(*bank.AccountsModel).Count(), proj.Position())

	// --- Snapshot corruption -> full replay fallback ----------------------
	loaded, info, err := repos.Load(ctx, "acct-1")
	must(err)
	fmt.Printf("normal load: mode=%s snapshotVersion=%d replayed=%d balance=%d\n",
		info.Mode, info.SnapshotVersion, info.ReplayedCount, loaded.(*bank.Account).Balance())

	// Trash the snapshot file on disk (bad bytes, stale checksum).
	sp := filepath.Join(dir, "snapshots", "bank.account__acct-1.json")
	must(os.WriteFile(sp, []byte("{not-json"), 0o644))
	loaded2, info2, err := repos.Load(ctx, "acct-1")
	must(err)
	fmt.Printf("corrupted snapshot load: mode=%s replayed=%d balance=%d\n",
		info2.Mode, info2.ReplayedCount, loaded2.(*bank.Account).Balance())
	if loaded2.(*bank.Account).Balance() != view2.Balance {
		fail(fmt.Errorf("fallback balance mismatch"))
	}

	fmt.Println("demo finished OK")
}

func must(err error) {
	if err != nil {
		fail(err)
	}
}

func must2(v bool) {
	if !v {
		fail(errors.New("expected true"))
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "demo failed:", err)
	os.Exit(1)
}

func waitUntil(ok func() bool) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	fail(errors.New("condition not met in time"))
}

func mustLast(s eventstore.EventStore) int64 {
	p, err := s.LastPosition(context.Background())
	must(err)
	return p
}

// plantV1Opened appends an old-schema opening + deposit as a legacy v1
// deployment would have produced them.
func plantV1Opened(store eventstore.EventStore) error {
	_, err := bank.AppendLegacyV1Events(context.Background(), store, "acct-legacy", 0,
		bank.LegacyV1Opened("acct-legacy", "Grace Hopper", "EUR"),
		bank.LegacyV1Deposit(250, "legacy book transfer"),
	)
	return err
}

// sameView compares the business fields of two account views (ignores the
// bookkeeping id set, which is rebuilt independently anyway).
func sameView(a, b bank.AccountView) bool {
	return a.ID == b.ID && a.Owner == b.Owner && a.Currency == b.Currency &&
		a.AccountType == b.AccountType && a.CreditLimit == b.CreditLimit &&
		a.Balance == b.Balance && a.DepositCount == b.DepositCount &&
		a.DepositTotal == b.DepositTotal && a.WithdrawTotal == b.WithdrawTotal
}

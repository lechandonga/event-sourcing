package event_sourcing_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/event-sourcing/bank"
	"github.com/lechandonga/event-sourcing/eventstore"
	"github.com/lechandonga/event-sourcing/projection"
	"github.com/lechandonga/event-sourcing/repo"
	"github.com/lechandonga/event-sourcing/snapshot"
)

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func waitPos(t *testing.T, p *projection.Projector, want int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p.Position() >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("projector at %d, want %d; failure=%+v", p.Position(), want, p.Failure())
}

func openAndDeposit(ctx context.Context, t *testing.T, r *repo.Repository, id, owner string, amount int64) {
	t.Helper()
	agg, _, err := r.Load(ctx, id)
	must(t, err)
	a := agg.(*bank.Account)
	if a.Version() == 0 {
		must(t, a.Open(owner, "USD", "checking", 0))
	}
	if amount > 0 {
		must(t, a.Deposit(amount, "test", "wire", ""))
	}
	_, _, err = r.Save(ctx, a)
	must(t, err)
}

func TestIntegration_ConcurrentVersionConflicts(t *testing.T) {
	ctx := context.Background()
	st := eventstore.NewMemoryEventStore()
	snaps := snapshot.NewMemoryStore()
	r := repo.New(st, snaps, bank.NewRegistry(),
		func(id string) repo.Aggregate { return bank.NewAccount(id) },
		repo.Options{SnapshotInterval: 100})

	openAndDeposit(ctx, t, r, "hot", "Ada", 100) // v1,v2

	const writers = 24
	var wg sync.WaitGroup
	var wins, conflicts int64
	var mu sync.Mutex
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attempt := 0; attempt < 100; attempt++ {
				agg, _, err := r.Load(ctx, "hot")
				if err != nil {
					t.Errorf("load %v", err)
					return
				}
				must := agg.(*bank.Account).Deposit(1, "", "wire", "")
				if must != nil {
					t.Errorf("deposit %v", must)
					return
				}
				_, _, err = r.Save(ctx, agg)
				if err == nil {
					mu.Lock()
					wins++
					mu.Unlock()
					return
				}
				var ce *eventstore.ConflictError
				if errors.As(err, &ce) {
					mu.Lock()
					conflicts++
					mu.Unlock()
					continue
				}
				t.Errorf("save %v", err)
				return
			}
			t.Errorf("writer exhausted retries")
		}()
	}
	wg.Wait()
	if wins != writers {
		t.Fatalf("wins=%d want %d (conflicts=%d)", wins, writers, conflicts)
	}
	if conflicts == 0 {
		t.Fatal("expected at least one observed conflict under contention")
	}
	final, _, err := r.Load(ctx, "hot")
	must(t, err)
	if got := final.(*bank.Account).Balance(); got != 100+writers {
		t.Fatalf("balance=%d want %d", got, 100+writers)
	}
	if got := final.(*bank.Account).Version(); got != 2+writers {
		t.Fatalf("version=%d want %d", got, 2+writers)
	}
}

func TestIntegration_CrossVersionReplay(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"memory", "file"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			var st eventstore.EventStore
			var snaps snapshot.Store
			var cps projection.CheckpointStore
			if mode == "memory" {
				st = eventstore.NewMemoryEventStore()
				snaps = snapshot.NewMemoryStore()
				cps = projection.NewMemoryCheckpointStore()
			} else {
				var err error
				st, err = eventstore.OpenFileEventStore(filepath.Join(dir, "store"))
				must(t, err)
				snaps = snapshot.NewFileStore(dir)
				cps = projection.NewFileCheckpointStore(dir)
			}
			reg := bank.NewRegistry()
			r := repo.New(st, snaps, reg,
				func(id string) repo.Aggregate { return bank.NewAccount(id) },
				repo.Options{SnapshotInterval: 100})

			// Ancient v1 history.
			_, err := bank.AppendLegacyV1Events(ctx, st, "legacy", 0,
				bank.LegacyV1Opened("legacy", "Grace", "EUR"),
				bank.LegacyV1Deposit(500, "v1 book"),
				bank.LegacyV1Withdrawal(80, "v1 wd"),
			)
			must(t, err)

			// Modern v2 commands continue the same stream.
			agg, info, err := r.Load(ctx, "legacy")
			must(t, err)
			if info.Mode != "full_replay" {
				t.Fatalf("mode=%s", info.Mode)
			}
			a := agg.(*bank.Account)
			if a.Balance() != 420 {
				t.Fatalf("v1 replay balance=%d want 420", a.Balance())
			}
			if a.AccountType() != bank.DefaultAccountType {
				t.Fatalf("upcast type=%q", a.AccountType())
			}
			must(t, a.Deposit(80, "v2 topup", "wire", "ref-x"))
			_, _, err = r.Save(ctx, a)
			must(t, err)

			// Projection over v1+v2 mix.
			p, err := projection.New("accounts", st, cps, bank.NewAccountsModel,
				projection.Options{Registry: reg, PollInterval: 2 * time.Millisecond})
			must(t, err)
			runCtx, cancel := context.WithCancel(ctx)
			go func() { _ = p.Run(runCtx) }()
			last, _ := st.LastPosition(ctx)
			waitPos(t, p, last)

			view, ok := p.Model().(*bank.AccountsModel).Account("legacy")
			if !ok {
				t.Fatal("legacy account missing from projection")
			}
			if view.Balance != 500 { // 500 - 80 + 80
				t.Fatalf("projection balance=%d want 500", view.Balance)
			}
			if view.DepositCount != 2 {
				t.Fatalf("depositCount=%d want 2 (v1 event counted too)", view.DepositCount)
			}
			if view.AccountType != bank.DefaultAccountType {
				t.Fatalf("projected type=%q", view.AccountType)
			}

			// Stored bytes remain v1 for the old events.
			all, _ := st.ReadAll(ctx, 0, 0)
			v1Count := 0
			for _, e := range all {
				if e.AggregateID == "legacy" && e.SchemaVersion == 1 {
					v1Count++
					var m map[string]json.RawMessage
					if err := json.Unmarshal(e.Payload, &m); err != nil {
						t.Fatal(err)
					}
					if _, ok := m["channel"]; ok {
						t.Fatal("historical payload was rewritten")
					}
				}
			}
			if v1Count != 3 {
				t.Fatalf("v1 stored events=%d want 3", v1Count)
			}
			cancel()
			if c, ok := st.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		})
	}
}

func TestIntegration_RebuildMatchesIncremental(t *testing.T) {
	ctx := context.Background()
	st := eventstore.NewMemoryEventStore()
	snaps := snapshot.NewMemoryStore()
	cps := projection.NewMemoryCheckpointStore()
	reg := bank.NewRegistry()
	r := repo.New(st, snaps, reg,
		func(id string) repo.Aggregate { return bank.NewAccount(id) },
		repo.Options{SnapshotInterval: 4})

	p, err := projection.New("accounts", st, cps, bank.NewAccountsModel,
		projection.Options{Registry: reg, PollInterval: time.Millisecond})
	must(t, err)
	runCtx, cancel := context.WithCancel(ctx)
	go func() { _ = p.Run(runCtx) }()

	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("acc-%02d", i%5)
		openAndDeposit(ctx, t, r, id, "owner", int64(10+i))
		last, _ := st.LastPosition(ctx)
		waitPos(t, p, last)
	}
	cancel()
	time.Sleep(5 * time.Millisecond)

	before := p.Model().(*bank.AccountsModel).All()
	must(t, p.Rebuild(ctx))
	after := p.Model().(*bank.AccountsModel).All()
	if len(before) != len(after) {
		t.Fatalf("account count %d != %d", len(before), len(after))
	}
	for i := range before {
		b, a := before[i], after[i]
		if b.ID != a.ID || b.Balance != a.Balance || b.DepositCount != a.DepositCount ||
			b.DepositTotal != a.DepositTotal || b.WithdrawTotal != a.WithdrawTotal ||
			b.AccountType != a.AccountType || b.CreditLimit != a.CreditLimit {
			t.Fatalf("view mismatch for %s:\n%+v\n%+v", b.ID, b, a)
		}
	}
	last, _ := st.LastPosition(ctx)
	if p.Position() != last {
		t.Fatalf("position=%d last=%d", p.Position(), last)
	}
}

func TestIntegration_SnapshotFallbackScenarios(t *testing.T) {
	ctx := context.Background()

	t.Run("missing snapshot", func(t *testing.T) {
		st := eventstore.NewMemoryEventStore()
		r := repo.New(st, snapshot.NewMemoryStore(), bank.NewRegistry(),
			func(id string) repo.Aggregate { return bank.NewAccount(id) },
			repo.Options{SnapshotInterval: 100})
		for i := 0; i < 6; i++ {
			openAndDeposit(ctx, t, r, "a1", "Ada", 10)
		}
		_, info, err := r.Load(ctx, "a1")
		must(t, err)
		if info.Mode != "full_replay" || info.ReplayedCount != 7 {
			t.Fatalf("info=%+v", info)
		}
	})

	t.Run("corrupted snapshot", func(t *testing.T) {
		st := eventstore.NewMemoryEventStore()
		snaps := snapshot.NewMemoryStore()
		r := repo.New(st, snaps, bank.NewRegistry(),
			func(id string) repo.Aggregate { return bank.NewAccount(id) },
			repo.Options{SnapshotInterval: 3})
		for i := 0; i < 8; i++ {
			openAndDeposit(ctx, t, r, "a1", "Ada", 10)
		}
		snaps.CorruptTest(bank.AggregateType, "a1", []byte(`garbage-state`))
		agg, info, err := r.Load(ctx, "a1")
		must(t, err)
		if info.Mode != "full_replay" || info.ReplayedCount != 9 {
			t.Fatalf("info=%+v", info)
		}
		if agg.(*bank.Account).Balance() != 80 {
			t.Fatalf("balance=%d", agg.(*bank.Account).Balance())
		}
	})

	t.Run("file snapshot corrupted on disk", func(t *testing.T) {
		dir := t.TempDir()
		st, err := eventstore.OpenFileEventStore(filepath.Join(dir, "store"))
		must(t, err)
		defer st.Close()
		snaps := snapshot.NewFileStore(dir)
		r := repo.New(st, snaps, bank.NewRegistry(),
			func(id string) repo.Aggregate { return bank.NewAccount(id) },
			repo.Options{SnapshotInterval: 3})
		for i := 0; i < 6; i++ {
			openAndDeposit(ctx, t, r, "a1", "Ada", 10)
		}
		agg, info, err := r.Load(ctx, "a1")
		must(t, err)
		if info.Mode != "snapshot+replay" {
			t.Fatalf("expected snapshot use, info=%+v", info)
		}
		must(t, snaps.CorruptTest(bank.AggregateType, "a1"))
		agg2, info2, err := r.Load(ctx, "a1")
		must(t, err)
		if info2.Mode != "full_replay" {
			t.Fatalf("expected full replay after on-disk corruption, info=%+v", info2)
		}
		if agg2.(*bank.Account).Balance() != agg.(*bank.Account).Balance() {
			t.Fatal("balances diverge after fallback")
		}
	})
}

func TestIntegration_EndToEndAcrossProcessRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Process 1: write events + run projection to the end.
	func() {
		st, err := eventstore.OpenFileEventStore(filepath.Join(dir, "store"))
		must(t, err)
		defer st.Close()
		snaps := snapshot.NewFileStore(dir)
		cps := projection.NewFileCheckpointStore(dir)
		mss := projection.NewFileStateStore(dir)
		reg := bank.NewRegistry()
		r := repo.New(st, snaps, reg,
			func(id string) repo.Aggregate { return bank.NewAccount(id) },
			repo.Options{SnapshotInterval: 5})
		for i := 0; i < 12; i++ {
			openAndDeposit(ctx, t, r, "a1", "Ada", 10)
		}
		p, err := projection.New("accounts", st, cps, bank.NewAccountsModel,
			projection.Options{Registry: reg, PollInterval: time.Millisecond,
				StateStore: mss, StateSaveInterval: 1})
		must(t, err)
		runCtx, cancel := context.WithCancel(ctx)
		go func() { _ = p.Run(runCtx) }()
		last, _ := st.LastPosition(ctx)
		waitPos(t, p, last)
		cancel()
		time.Sleep(5 * time.Millisecond)
	}()

	// Process 2: reopen everything; aggregate loads from snapshot+tail,
	// projection resumes from checkpoint and sees no duplicates.
	st2, err := eventstore.OpenFileEventStore(filepath.Join(dir, "store"))
	must(t, err)
	defer st2.Close()
	snaps2 := snapshot.NewFileStore(dir)
	cps2 := projection.NewFileCheckpointStore(dir)
	mss2 := projection.NewFileStateStore(dir)
	reg := bank.NewRegistry()
	r := repo.New(st2, snaps2, reg,
		func(id string) repo.Aggregate { return bank.NewAccount(id) },
		repo.Options{SnapshotInterval: 5})
	agg, info, err := r.Load(ctx, "a1")
	must(t, err)
	if agg.(*bank.Account).Balance() != 120 {
		t.Fatalf("balance after restart=%d", agg.(*bank.Account).Balance())
	}
	if info.Mode != "snapshot+replay" {
		t.Fatalf("expected snapshot+replay, got %+v", info)
	}

	p, err := projection.New("accounts", st2, cps2, bank.NewAccountsModel,
		projection.Options{Registry: reg, PollInterval: time.Millisecond,
			StateStore: mss2, StateSaveInterval: 1})
	must(t, err)
	if p.Position() == 0 {
		t.Fatal("checkpoint/state did not survive restart")
	}
	// append one new event and ensure only it gets applied (no reprocessing)
	openAndDeposit(ctx, t, r, "a1", "Ada", 7)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = p.Run(runCtx) }()
	last, _ := st2.LastPosition(ctx)
	waitPos(t, p, last)
	view, _ := p.Model().(*bank.AccountsModel).Account("a1")
	if view.Balance != 127 {
		t.Fatalf("balance=%d want 127", view.Balance)
	}
	stats := p.Stats()
	if stats.Applied != 1 {
		t.Fatalf("applied=%d after resume, want exactly 1 (no duplicates)", stats.Applied)
	}
}

func TestIntegration_LogFilesExistLocally(t *testing.T) {
	dir := t.TempDir()
	st, err := eventstore.OpenFileEventStore(filepath.Join(dir, "store"))
	must(t, err)
	ctx := context.Background()
	_, err = st.Append(ctx, bank.AggregateType, "a1", 0, []eventstore.UncommittedEvent{
		bank.LegacyV1Opened("a1", "Ada", "USD"),
	})
	must(t, err)
	must(t, st.Close())
	data, err := os.ReadFile(filepath.Join(dir, "store", "events.log"))
	must(t, err)
	if len(data) == 0 {
		t.Fatal("event log empty")
	}
}

package bank_test

import (
	"context"
	"testing"
	"time"

	"github.com/lechandonga/event-sourcing/bank"
	"github.com/lechandonga/event-sourcing/eventstore"
	"github.com/lechandonga/event-sourcing/projection"
)

// TestBankReadModelRebuildAcrossVersions：混合 v1/v2/v3 事件，
// 增量读模型与从完整历史重建的读模型必须完全一致；
// 且重放计数恰好等于事件数（重复投递不重复计数）。
func TestBankReadModelRebuildAcrossVersions(t *testing.T) {
	ctx := context.Background()
	st := eventstore.NewMemoryStore()
	registry := bank.NewRegistry()

	// 三个账户，分别以 v1 / v2 / 当前版本开户
	seed := []struct {
		id      string
		openRaw []byte
	}{
		{"old-1", bank.AccountOpenedV1{Holder: "A", InitialBalance: 10}.MarshalV1()}, // 10 元
		{"old-2", bank.AccountOpenedV2{Holder: "B", InitialBalanceCents: 2000}.MarshalV2()},
	}
	for _, s := range seed {
		sid := eventstore.StreamID{Type: bank.AccountType, ID: s.id}
		if _, err := st.Append(ctx, sid, eventstore.ExpectedVersionNew,
			[]eventstore.UncommittedEvent{{Type: "bank.AccountOpened", Data: s.openRaw}}); err != nil {
			t.Fatal(err)
		}
	}
	repoInst := bank.NewAccountRepository(bank.AccountRepositoryConfig{Store: st, Registry: registry})
	newAcc, err := bank.OpenAccount("new-1", "C", 500, "EUR")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bank.SaveAccount(ctx, repoInst, newAcc); err != nil {
		t.Fatal(err)
	}

	// 对 old-1 追加 v1 存款（2 元），old-2 追加 v2 存款（300 分），new-1 当前版
	_, err = st.Append(ctx,
		eventstore.StreamID{Type: bank.AccountType, ID: "old-1"},
		eventstore.ExpectedVersion(1),
		[]eventstore.UncommittedEvent{{Type: "bank.MoneyDeposited", Data: bank.MoneyV1{Amount: 2}.MarshalV1()}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.Append(ctx,
		eventstore.StreamID{Type: bank.AccountType, ID: "old-2"},
		eventstore.ExpectedVersion(1),
		[]eventstore.UncommittedEvent{{Type: "bank.MoneyDeposited", Data: bank.MoneyV2{AmountCents: 300}.MarshalV2()}})
	if err != nil {
		t.Fatal(err)
	}
	a, err := bank.LoadAccount(ctx, repoInst, "new-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Withdraw(120, "cash"); err != nil {
		t.Fatal(err)
	}
	if _, err := bank.SaveAccount(ctx, repoInst, a); err != nil {
		t.Fatal(err)
	}

	mkProj := func(name string) (*bank.SummaryReadModel, *projection.Projector) {
		m := bank.NewSummaryReadModel()
		p, err := projection.New(projection.Config{
			Name: name, Store: st, Model: m, Registry: registry,
			Checkpoints:  projection.NewMemoryCheckpointStore(),
			Failures:     projection.NewMemoryFailureStore(),
			PollInterval: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		return m, p
	}

	incModel, inc := mkProj("inc")
	if err := inc.CatchUp(ctx); err != nil {
		t.Fatal(err)
	}

	repModel, rep := mkProj("rep")
	if err := rep.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}

	incSnap, repSnap := incModel.Snapshot(), repModel.Snapshot()
	if len(incSnap) != len(repSnap) {
		t.Fatalf("account count differs: %d vs %d", len(incSnap), len(repSnap))
	}
	for id, want := range incSnap {
		got, ok := repSnap[id]
		if !ok {
			t.Fatalf("rebuild missing account %s", id)
		}
		if got != want {
			t.Fatalf("account %s differs: incremental=%+v rebuild=%+v", id, want, got)
		}
	}
	// 具体升级语义断言
	old1 := repSnap["old-1"]
	if old1.BalanceCents != 1200 || old1.Currency != bank.DefaultCurrency || old1.OpenedVersion != 1 {
		t.Fatalf("v1 account upcast wrong: %+v", old1)
	}
	old2 := repSnap["old-2"]
	if old2.BalanceCents != 2300 || old2.Currency != bank.DefaultCurrency {
		t.Fatalf("v2 account upcast wrong: %+v", old2)
	}
	new1 := repSnap["new-1"]
	if new1.BalanceCents != 380 || new1.Currency != "EUR" || new1.WithdrawalCount != 1 {
		t.Fatalf("v3 account wrong: %+v", new1)
	}
	// 幂等：重建后再 CatchUp 不应重复计数
	before := incModel.TotalApplied()
	if err := inc.CatchUp(ctx); err != nil {
		t.Fatal(err)
	}
	if incModel.TotalApplied() != before {
		t.Fatalf("catch-up after caught up applied duplicate events: %d -> %d",
			before, incModel.TotalApplied())
	}
}

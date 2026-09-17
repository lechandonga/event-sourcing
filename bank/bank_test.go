package bank_test

import (
	"context"
	"testing"

	"github.com/lechandonga/event-sourcing/bank"
	"github.com/lechandonga/event-sourcing/eventstore"
	"github.com/lechandonga/event-sourcing/snapshot"
)

// TestCrossVersionReplay 直接把 v1/v2 原始结构写入事件流（模拟十年前的提交），
// 再通过仓储加载与投影读取：历史事件必须先升级到当前 v3 再参与重放，
// 新增字段（currency/note）有确定缺省语义，金额单位换算（元->分，×100）精确。
func TestCrossVersionReplay(t *testing.T) {
	ctx := context.Background()
	st := eventstore.NewMemoryStore()
	registry := bank.NewRegistry()
	sid := eventstore.StreamID{Type: bank.AccountType, ID: "acct-hist"}

	// 1) v1 开户：10 元
	_, err := st.Append(ctx, sid, eventstore.ExpectedVersionNew,
		[]eventstore.UncommittedEvent{{Type: "bank.AccountOpened", Data: bank.AccountOpenedV1{
			Holder: "Grace", InitialBalance: 10,
		}.MarshalV1()}})
	if err != nil {
		t.Fatal(err)
	}
	// 2) v2 开户中间态不会单独存在；继续追加 v1 存款 3 元
	_, err = st.Append(ctx, sid, eventstore.ExpectedVersion(1),
		[]eventstore.UncommittedEvent{{Type: "bank.MoneyDeposited", Data: bank.MoneyV1{Amount: 3}.MarshalV1()}})
	if err != nil {
		t.Fatal(err)
	}
	// 3) v2 取款 200 分（无 note 字段）
	_, err = st.Append(ctx, sid, eventstore.ExpectedVersion(2),
		[]eventstore.UncommittedEvent{{Type: "bank.MoneyWithdrawn", Data: bank.MoneyV2{AmountCents: 200}.MarshalV2()}})
	if err != nil {
		t.Fatal(err)
	}
	// 4) 当前版本存款（注册表编码，带 schemaVersion=3）
	openRaw := currentV3(t, &bank.MoneyDeposited{AmountCents: 50, Note: "tip"})
	_, err = st.Append(ctx, sid, eventstore.ExpectedVersion(3),
		[]eventstore.UncommittedEvent{{Type: "bank.MoneyDeposited", Data: openRaw}})
	if err != nil {
		t.Fatal(err)
	}

	// 原始历史事件的内容必须仍保持 v1（升级不改写历史；字段顺序以结构化比较为准）。
	raw, _ := st.ReadStream(ctx, sid, 0, 1)
	var hist map[string]any
	if err := jsonUnmarshal(raw[0].Data, &hist); err != nil {
		t.Fatal(err)
	}
	if hist["schemaVersion"] != float64(1) || hist["initialBalance"] != float64(10) ||
		hist["holder"] != "Grace" {
		t.Fatalf("committed history was rewritten: %s", raw[0].Data)
	}
	if _, hasNewField := hist["initialBalanceCents"]; hasNewField {
		t.Fatalf("history must not contain new field, got %s", raw[0].Data)
	}

	repoInst := bank.NewAccountRepository(bank.AccountRepositoryConfig{
		Store: st, Registry: registry,
		Snapshots: snapshot.NewMemoryStore(), Threshold: 100, // 不会触发，强制全量重放
	})
	loaded, err := bank.LoadAccount(ctx, repoInst, "acct-hist")
	if err != nil {
		t.Fatal(err)
	}
	// 10 元*100 + 3 元*100 - 200 分 + 50 分 = 1150 分
	if loaded.BalanceCents() != 1150 {
		t.Fatalf("replayed balance=%d, want 1150", loaded.BalanceCents())
	}
	if loaded.Currency() != bank.DefaultCurrency {
		t.Fatalf("v1 account must upcast to default currency %s, got %s",
			bank.DefaultCurrency, loaded.Currency())
	}
	if loaded.Version() != 4 {
		t.Fatalf("version=%d want 4", loaded.Version())
	}
}

// TestCurrentWritesCarryV3 保证今天产生的新事件都带当前 schemaVersion。
func TestCurrentWritesCarryV3(t *testing.T) {
	ctx := context.Background()
	st := eventstore.NewMemoryStore()
	registry := bank.NewRegistry()
	repoInst := bank.NewAccountRepository(bank.AccountRepositoryConfig{
		Store: st, Registry: registry,
		Snapshots: snapshot.NewMemoryStore(), Threshold: 100,
	})
	a, err := bank.OpenAccount("a9", "Hopper", 0, "")
	if err != nil {
		t.Fatal(err)
	}
	if a.Currency() != bank.DefaultCurrency {
		t.Fatalf("default currency not applied: %q", a.Currency())
	}
	if _, err := bank.SaveAccount(ctx, repoInst, a); err != nil {
		t.Fatal(err)
	}
	if err := a.Deposit(100, "gift"); err != nil {
		t.Fatal(err)
	}
	if _, err := bank.SaveAccount(ctx, repoInst, a); err != nil {
		t.Fatal(err)
	}
	events, _ := st.ReadStream(ctx, eventstore.StreamID{Type: bank.AccountType, ID: "a9"}, 0, 0)
	for _, e := range events {
		var m map[string]any
		if err := jsonUnmarshal(e.Data, &m); err != nil {
			t.Fatal(err)
		}
		if m["schemaVersion"] != float64(3) {
			t.Fatalf("event %s stored with schemaVersion=%v, want 3", e.Type, m["schemaVersion"])
		}
	}
}

// TestWithdrawOverdraftRejected 余额不足时写侧不产生事件。
func TestWithdrawOverdraftRejected(t *testing.T) {
	a, err := bank.OpenAccount("a1", "X", 50, "USD")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Withdraw(60, ""); err == nil {
		t.Fatal("overdraft must be rejected")
	}
	if len(a.PendingEvents()) != 1 { // 只有开户事件
		t.Fatalf("rejected command must not add pending events, got %d", len(a.PendingEvents()))
	}
	if a.BalanceCents() != 50 {
		t.Fatal("rejected command must not mutate balance")
	}
}

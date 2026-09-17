package bank_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/lechandonga/event-sourcing/bank"
	"github.com/lechandonga/event-sourcing/eventstore"
)

func TestRegistryUpcastsV1HistoryToV2(t *testing.T) {
	r := bank.NewRegistry()
	v1 := json.RawMessage(`{"account_id":"a1","owner":"Ada","currency":"USD"}`)
	v, err := r.Decode(bank.TypeAccountOpened, bank.SchemaV1, v1)
	if err != nil {
		t.Fatal(err)
	}
	open := v.(*bank.AccountOpenedV2)
	if open.AccountType != bank.DefaultAccountType || open.CreditLimit != 0 {
		t.Fatalf("defaults wrong: %+v", open)
	}

	d, err := r.Decode(bank.TypeMoneyDeposited, bank.SchemaV1, json.RawMessage(`{"amount":50,"note":"n"}`))
	if err != nil {
		t.Fatal(err)
	}
	dep := d.(*bank.MoneyDepositedV2)
	if dep.Channel != bank.DefaultChannel || dep.ExternalRef != "" || dep.Amount != 50 {
		t.Fatalf("deposit defaults wrong: %+v", dep)
	}

	w, err := r.Decode(bank.TypeMoneyWithdrawn, bank.SchemaV1, json.RawMessage(`{"amount":10,"note":"n"}`))
	if err != nil {
		t.Fatal(err)
	}
	wd := w.(*bank.MoneyWithdrawnV2)
	if wd.Channel != bank.DefaultChannel || wd.ReasonCode != bank.DefaultReasonCode {
		t.Fatalf("withdraw defaults wrong: %+v", wd)
	}
}

func TestStoredV1BytesAreNotRewritten(t *testing.T) {
	st := eventstore.NewMemoryEventStore()
	ctx := context.Background()
	_, err := bank.AppendLegacyV1Events(ctx, st, "legacy", 0,
		bank.LegacyV1Opened("legacy", "Ada", "USD"),
		bank.LegacyV1Deposit(100, "old note"),
		bank.LegacyV1Withdrawal(30, "old wd"),
	)
	if err != nil {
		t.Fatal(err)
	}
	all, _ := st.ReadAll(ctx, 0, 0)
	for _, e := range all {
		if e.SchemaVersion != bank.SchemaV1 {
			t.Fatalf("stored schema changed: %d", e.SchemaVersion)
		}
		var m map[string]json.RawMessage
		if err := json.Unmarshal(e.Payload, &m); err != nil {
			t.Fatal(err)
		}
		if _, hasV2Field := m["channel"]; hasV2Field {
			t.Fatalf("v1 payload was rewritten with v2 field: %s", e.Payload)
		}
	}
}

func TestAccountInvariants(t *testing.T) {
	a := bank.NewAccount("a1")
	if err := a.Deposit(10, "", "", ""); err == nil {
		t.Fatal("deposit on unopened account must fail")
	}
	if err := a.Open("Ada", "USD", "", 0); err != nil {
		t.Fatal(err)
	}
	if a.AccountType() != bank.DefaultAccountType {
		t.Fatalf("default account type = %q", a.AccountType())
	}
	if err := a.Open("Ada", "USD", "", 0); err == nil {
		t.Fatal("double open must fail")
	}
	if err := a.Deposit(-5, "", "", ""); err == nil {
		t.Fatal("non-positive deposit must fail")
	}
	if err := a.Deposit(100, "", "", ""); err != nil {
		t.Fatal(err)
	}
	if err := a.Withdraw(101, "", "", ""); err == nil {
		t.Fatal("overdraft beyond credit limit must fail")
	}
	if err := a.Withdraw(100, "", "", ""); err != nil {
		t.Fatalf("within funds withdrawal failed: %v", err)
	}
	if a.Balance() != 0 {
		t.Fatalf("balance=%d", a.Balance())
	}
	// credit limit allows borrowing
	b := bank.NewAccount("a2")
	if err := b.Open("Bob", "USD", "checking", 500); err != nil {
		t.Fatal(err)
	}
	if err := b.Withdraw(300, "", "", ""); err != nil {
		t.Fatalf("credit withdrawal failed: %v", err)
	}
	if b.Balance() != -300 {
		t.Fatalf("balance=%d want -300", b.Balance())
	}
}

func TestRebuildReplayingV1AndV2MixProducesSameAggregateState(t *testing.T) {
	st := eventstore.NewMemoryEventStore()
	ctx := context.Background()
	reg := bank.NewRegistry()

	// v1 history
	_, err := bank.AppendLegacyV1Events(ctx, st, "mix", 0,
		bank.LegacyV1Opened("mix", "Ada", "USD"),
		bank.LegacyV1Deposit(100, "v1 deposit"),
		bank.LegacyV1Withdrawal(20, "v1 wd"),
	)
	if err != nil {
		t.Fatal(err)
	}
	// v2 continuation through the aggregate
	acc := bank.NewAccount("mix")
	events, _ := st.ReadStream(ctx, bank.AggregateType, "mix", eventstore.ReadOptions{})
	for _, e := range events {
		v, err := reg.Decode(e.EventType, e.SchemaVersion, e.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if err := acc.ApplyEvent(e, v); err != nil {
			t.Fatal(err)
		}
	}
	if acc.Balance() != 80 {
		t.Fatalf("balance after v1 replay=%d", acc.Balance())
	}
	if err := acc.Deposit(20, "v2 deposit", "wire", "ref-1"); err != nil {
		t.Fatal(err)
	}
	committed, err := st.Append(ctx, bank.AggregateType, "mix", acc.Version(), acc.PullPending())
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range committed {
		v, _ := reg.Decode(e.EventType, e.SchemaVersion, e.Payload)
		if err := acc.ApplyEvent(e, v); err != nil {
			t.Fatal(err)
		}
	}
	if acc.Balance() != 100 {
		t.Fatalf("balance=%d want 100", acc.Balance())
	}

	// Re-replay everything from zero using only the store: must converge.
	fresh := bank.NewAccount("mix")
	all, _ := st.ReadStream(ctx, bank.AggregateType, "mix", eventstore.ReadOptions{})
	for _, e := range all {
		v, err := reg.Decode(e.EventType, e.SchemaVersion, e.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if err := fresh.ApplyEvent(e, v); err != nil {
			t.Fatal(err)
		}
	}
	if fresh.Balance() != 100 || fresh.Version() != 4 || fresh.AccountType() != bank.DefaultAccountType {
		t.Fatalf("re-replay mismatch: balance=%d version=%d type=%q", fresh.Balance(), fresh.Version(), fresh.AccountType())
	}
}

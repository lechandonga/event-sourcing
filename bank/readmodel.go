package bank

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/lechandonga/event-sourcing/eventstore"
	"github.com/lechandonga/event-sourcing/projection"
)

// AccountView is one row of the accounts read model.
type AccountView struct {
	ID             string
	Owner          string
	Currency       string
	AccountType    string
	CreditLimit    int64
	Balance        int64
	DepositCount   int64
	DepositTotal   int64
	WithdrawTotal  int64
	OpenedAt       string
	LastEventID    string
	AppliedEventID map[string]struct{} // durable-style idempotency set
}

// AccountsModel is the bank accounts projection. It applies events in global
// order and keeps an explicit per-event id set so that double delivery (which
// the projector normally filters, and which can also come from an external
// redelivery) can never double count.
type AccountsModel struct {
	mu    sync.RWMutex
	byID  map[string]*AccountView
	order []string
}

// NewAccountsModel is the projection.ModelFactory-compatible constructor.
func NewAccountsModel() projection.ReadModel {
	return &AccountsModel{byID: make(map[string]*AccountView)}
}

// Handle implements projection.ReadModel.
func (m *AccountsModel) Handle(_ context.Context, env eventstore.Envelope, decoded any) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch p := decoded.(type) {
	case *AccountOpenedV2:
		if _, exists := m.byID[p.AccountID]; exists {
			// Idempotent: an open event already folded this account.
			return nil
		}
		m.byID[p.AccountID] = &AccountView{
			ID:             p.AccountID,
			Owner:          p.Owner,
			Currency:       p.Currency,
			AccountType:    p.AccountType,
			CreditLimit:    p.CreditLimit,
			OpenedAt:       env.Timestamp.Format("2006-01-02T15:04:05Z07:00"),
			LastEventID:    env.EventID,
			AppliedEventID: map[string]struct{}{env.EventID: {}},
		}
		m.order = append(m.order, p.AccountID)
	case *MoneyDepositedV2:
		v := m.byID[env.AggregateID]
		if v == nil {
			return fmt.Errorf("bank projection: deposit for unknown account %s", env.AggregateID)
		}
		if _, dup := v.AppliedEventID[env.EventID]; dup {
			return nil
		}
		v.Balance += p.Amount
		v.DepositCount++
		v.DepositTotal += p.Amount
		v.LastEventID = env.EventID
		v.AppliedEventID[env.EventID] = struct{}{}
	case *MoneyWithdrawnV2:
		v := m.byID[env.AggregateID]
		if v == nil {
			return fmt.Errorf("bank projection: withdrawal for unknown account %s", env.AggregateID)
		}
		if _, dup := v.AppliedEventID[env.EventID]; dup {
			return nil
		}
		v.Balance -= p.Amount
		v.WithdrawTotal += p.Amount
		v.LastEventID = env.EventID
		v.AppliedEventID[env.EventID] = struct{}{}
	default:
		return fmt.Errorf("bank projection: unknown payload %T for %s", decoded, env.EventType)
	}
	return nil
}

// Account returns a copied snapshot of one account view.
func (m *AccountsModel) Account(id string) (AccountView, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.byID[id]
	if !ok {
		return AccountView{}, false
	}
	return *v, true
}

// All returns copied views in open order.
func (m *AccountsModel) All() []AccountView {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]AccountView, 0, len(m.order))
	for _, id := range m.order {
		out = append(out, *m.byID[id])
	}
	return out
}

// Count returns the number of accounts.
func (m *AccountsModel) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.byID)
}

// --- Stateful implementation (durable model-state snapshots) --------------

type persistedRow struct {
	ID             string   `json:"id"`
	Owner          string   `json:"owner"`
	Currency       string   `json:"currency"`
	AccountType    string   `json:"account_type"`
	CreditLimit    int64    `json:"credit_limit"`
	Balance        int64    `json:"balance"`
	DepositCount   int64    `json:"deposit_count"`
	DepositTotal   int64    `json:"deposit_total"`
	WithdrawTotal  int64    `json:"withdraw_total"`
	OpenedAt       string   `json:"opened_at"`
	LastEventID    string   `json:"last_event_id"`
	AppliedEventID []string `json:"applied_event_ids"`
}

type persistedModel struct {
	Order []string       `json:"order"`
	Rows  []persistedRow `json:"rows"`
}

// MarshalState implements projection.Stateful.
func (m *AccountsModel) MarshalState() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	pm := persistedModel{Order: append([]string(nil), m.order...)}
	for _, id := range m.order {
		v := m.byID[id]
		ids := make([]string, 0, len(v.AppliedEventID))
		for k := range v.AppliedEventID {
			ids = append(ids, k)
		}
		sort.Strings(ids)
		pm.Rows = append(pm.Rows, persistedRow{
			ID: v.ID, Owner: v.Owner, Currency: v.Currency, AccountType: v.AccountType,
			CreditLimit: v.CreditLimit, Balance: v.Balance, DepositCount: v.DepositCount,
			DepositTotal: v.DepositTotal, WithdrawTotal: v.WithdrawTotal,
			OpenedAt: v.OpenedAt, LastEventID: v.LastEventID, AppliedEventID: ids,
		})
	}
	data, err := json.Marshal(pm)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// UnmarshalState implements projection.Stateful.
func (m *AccountsModel) UnmarshalState(data []byte) error {
	var pm persistedModel
	if err := json.Unmarshal(data, &pm); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.order = append([]string(nil), pm.Order...)
	m.byID = make(map[string]*AccountView, len(pm.Rows))
	for i := range pm.Rows {
		r := &pm.Rows[i]
		set := make(map[string]struct{}, len(r.AppliedEventID))
		for _, id := range r.AppliedEventID {
			set[id] = struct{}{}
		}
		m.byID[r.ID] = &AccountView{
			ID: r.ID, Owner: r.Owner, Currency: r.Currency, AccountType: r.AccountType,
			CreditLimit: r.CreditLimit, Balance: r.Balance, DepositCount: r.DepositCount,
			DepositTotal: r.DepositTotal, WithdrawTotal: r.WithdrawTotal,
			OpenedAt: r.OpenedAt, LastEventID: r.LastEventID, AppliedEventID: set,
		}
	}
	return nil
}

// Package bank is the example domain: a simple bank account used to
// demonstrate event sourcing, schema evolution, projections and snapshots.
//
// Event catalog
//
//	account.opened
//	  v1: {account_id, owner, currency}
//	  v2: + account_type (default "checking"), + credit_limit (default 0)
//	money.deposited
//	  v1: {amount, note}
//	  v2: + channel (default "unknown"), + external_ref (default "")
//	money.withdrawn
//	  v1: {amount, note}
//	  v2: + channel (default "unknown"), + reason_code (default "NONE")
//
// Default semantics are part of the contract: every missing v1 field
// deterministically becomes the documented default, so the same v1 event
// always replays into the same v2-shaped behavior. Defaults are neutral
// values that preserve v1 business meaning (no credit line, unknown channel,
// standard checking account).
package bank

import (
	"encoding/json"
	"fmt"

	"github.com/lechandonga/event-sourcing/event"
)

// Event type names and current schema version.
const (
	TypeAccountOpened  = "bank.account.opened"
	TypeMoneyDeposited = "bank.money.deposited"
	TypeMoneyWithdrawn = "bank.money.withdrawn"

	SchemaV1 = 1
	SchemaV2 = 2
)

// --- Current (v2) payloads -------------------------------------------------

// AccountOpenedV2 is the current shape of bank.account.opened.
type AccountOpenedV2 struct {
	AccountID   string `json:"account_id"`
	Owner       string `json:"owner"`
	Currency    string `json:"currency"`
	AccountType string `json:"account_type"` // v2 addition; default "checking"
	CreditLimit int64  `json:"credit_limit"` // v2 addition; default 0
}

// MoneyDepositedV2 is the current shape of bank.money.deposited.
type MoneyDepositedV2 struct {
	Amount      int64  `json:"amount"`
	Note        string `json:"note"`
	Channel     string `json:"channel"`      // v2 addition; default "unknown"
	ExternalRef string `json:"external_ref"` // v2 addition; default ""
}

// MoneyWithdrawnV2 is the current shape of bank.money.withdrawn.
type MoneyWithdrawnV2 struct {
	Amount     int64  `json:"amount"`
	Note       string `json:"note"`
	Channel    string `json:"channel"`     // v2 addition; default "unknown"
	ReasonCode string `json:"reason_code"` // v2 addition; default "NONE"
}

// --- Historical v1 payloads (kept for explicit upgrade code) --------------

type accountOpenedV1 struct {
	AccountID string `json:"account_id"`
	Owner     string `json:"owner"`
	Currency  string `json:"currency"`
}

type moneyDepositedV1 struct {
	Amount int64  `json:"amount"`
	Note   string `json:"note"`
}

type moneyWithdrawnV1 struct {
	Amount int64  `json:"amount"`
	Note   string `json:"note"`
}

// Defaults introduced by v2. Exported so tests and docs can reference them.
const (
	DefaultAccountType = "checking"
	DefaultChannel     = "unknown"
	DefaultReasonCode  = "NONE"
)

func decode[T any](raw json.RawMessage) (any, error) {
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// NewRegistry builds the bank event registry with the full v1 -> v2 upgrade
// chain and typed decoders for the current schema.
func NewRegistry() *event.Registry {
	r := event.NewRegistry()

	r.Register(TypeAccountOpened, SchemaV2, func(raw json.RawMessage) (any, error) {
		return decode[AccountOpenedV2](raw)
	})
	r.Register(TypeMoneyDeposited, SchemaV2, func(raw json.RawMessage) (any, error) {
		return decode[MoneyDepositedV2](raw)
	})
	r.Register(TypeMoneyWithdrawn, SchemaV2, func(raw json.RawMessage) (any, error) {
		return decode[MoneyWithdrawnV2](raw)
	})

	// Upcasters decode into a generic map rather than a v1 struct: a v1
	// payload that already accidentally carried a v2 key keeps it, and any
	// extra unknown field is preserved unchanged.
	r.AddUpcaster(TypeAccountOpened, SchemaV1, func(raw json.RawMessage) (json.RawMessage, error) {
		m, err := toObject(raw)
		if err != nil {
			return nil, err
		}
		setDefault(m, "account_type", DefaultAccountType)
		setDefault(m, "credit_limit", float64(0))
		return json.Marshal(m)
	})
	r.AddUpcaster(TypeMoneyDeposited, SchemaV1, func(raw json.RawMessage) (json.RawMessage, error) {
		m, err := toObject(raw)
		if err != nil {
			return nil, err
		}
		setDefault(m, "channel", DefaultChannel)
		setDefault(m, "external_ref", "")
		return json.Marshal(m)
	})
	r.AddUpcaster(TypeMoneyWithdrawn, SchemaV1, func(raw json.RawMessage) (json.RawMessage, error) {
		m, err := toObject(raw)
		if err != nil {
			return nil, err
		}
		setDefault(m, "channel", DefaultChannel)
		setDefault(m, "reason_code", DefaultReasonCode)
		return json.Marshal(m)
	})

	if err := r.Validate(); err != nil {
		// Programming error: the chain above is static.
		panic(fmt.Sprintf("bank: invalid registry: %v", err))
	}
	return r
}

// toObject decodes raw into map form, rejecting non-objects defensively.
func toObject(raw json.RawMessage) (map[string]any, error) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("event payload is not a json object: %w", err)
	}
	if m == nil {
		m = map[string]any{}
	}
	return m, nil
}

// setDefault sets key only when absent OR when the v1 payload carried an
// explicit JSON null. This makes the upgrade stable and total.
func setDefault(m map[string]any, key string, def any) {
	if v, ok := m[key]; !ok || v == nil {
		m[key] = def
	}
}

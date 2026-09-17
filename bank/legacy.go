package bank

import (
	"context"
	"encoding/json"

	"github.com/lechandonga/event-sourcing/eventstore"
)

// LegacyV1Opened is an intentionally old-schema opening event, as it would
// have been written by a v1 deployment: no account_type, no credit_limit.
// Tests and the demo append it directly to prove upcast-on-read.
func LegacyV1Opened(id, owner, currency string) eventstore.UncommittedEvent {
	raw, _ := json.Marshal(map[string]any{
		"account_id": id,
		"owner":      owner,
		"currency":   currency,
	})
	return eventstore.UncommittedEvent{
		EventID:       eventstore.NewEventID(),
		EventType:     TypeAccountOpened,
		SchemaVersion: SchemaV1,
		Payload:       raw,
	}
}

// LegacyV1Deposit is an old-schema deposit (no channel/external_ref).
func LegacyV1Deposit(amount int64, note string) eventstore.UncommittedEvent {
	raw, _ := json.Marshal(map[string]any{
		"amount": amount,
		"note":   note,
	})
	return eventstore.UncommittedEvent{
		EventID:       eventstore.NewEventID(),
		EventType:     TypeMoneyDeposited,
		SchemaVersion: SchemaV1,
		Payload:       raw,
	}
}

// LegacyV1Withdrawal is an old-schema withdrawal (no channel/reason_code).
func LegacyV1Withdrawal(amount int64, note string) eventstore.UncommittedEvent {
	raw, _ := json.Marshal(map[string]any{
		"amount": amount,
		"note":   note,
	})
	return eventstore.UncommittedEvent{
		EventID:       eventstore.NewEventID(),
		EventType:     TypeMoneyWithdrawn,
		SchemaVersion: SchemaV1,
		Payload:       raw,
	}
}

// AppendLegacyV1Events appends a hand-built v1 batch directly to a store,
// bypassing the aggregate to simulate history produced by an old binary.
func AppendLegacyV1Events(ctx context.Context, store eventstore.EventStore, accountID string, expectedVersion int, events ...eventstore.UncommittedEvent) ([]eventstore.Envelope, error) {
	return store.Append(ctx, AggregateType, accountID, expectedVersion, events)
}

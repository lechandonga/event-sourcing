package bank

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lechandonga/event-sourcing/eventstore"
)

// AggregateType is the bank aggregate stream name.
const AggregateType = "bank.account"

// Domain errors returned by commands. These are invariant violations, not
// concurrency conflicts (the latter come from the event store).
var (
	ErrAlreadyOpen       = errors.New("bank: account already open")
	ErrNotOpen           = errors.New("bank: account not open")
	ErrInvalidAmount     = errors.New("bank: amount must be positive")
	ErrInsufficientFunds = errors.New("bank: insufficient funds")
	ErrInvalidCurrency   = errors.New("bank: currency mismatch")
)

// Account is the bank account aggregate.
type Account struct {
	id           string
	version      int
	open         bool
	owner        string
	currency     string
	accountType  string
	creditLimit  int64
	balance      int64
	totalIn      int64
	totalOut     int64
	depositCount int64
	pending      []eventstore.UncommittedEvent
	// pendingIDs is the id set of events already reflected in the command-side
	// state; when Save folds the committed envelopes back, ApplyEvent skips
	// the state mutation for these and only advances the version.
	pendingIDs map[string]struct{}
}

// NewAccount instantiates an unopened aggregate handle for loading.
func NewAccount(id string) *Account {
	return &Account{id: id, pendingIDs: map[string]struct{}{}}
}

// AggregateID implements repo.Aggregate.
func (a *Account) AggregateID() string { return a.id }

// AggregateType implements repo.Aggregate.
func (a *Account) AggregateType() string { return AggregateType }

// Version implements repo.Aggregate.
func (a *Account) Version() int { return a.version }

// SeekVersion implements repo.Aggregate: used after restoring snapshot state
// so that the following ApplyEvent calls expect snapshot version + 1.
func (a *Account) SeekVersion(v int) error {
	if v < 0 {
		return fmt.Errorf("bank: seek to negative version %d", v)
	}
	a.version = v
	return nil
}

// Balance returns the current balance (minor units).
func (a *Account) Balance() int64 { return a.balance }

// Currency returns the account currency.
func (a *Account) Currency() string { return a.currency }

// Owner returns the owner name.
func (a *Account) Owner() string { return a.owner }

// AccountType returns the v2 account type.
func (a *Account) AccountType() string { return a.accountType }

// CreditLimit returns the v2 credit limit.
func (a *Account) CreditLimit() int64 { return a.creditLimit }

// Stats returns flow counters.
type Stats struct {
	TotalDeposited int64
	TotalWithdrawn int64
	DepositCount   int64
}

// Stats returns money-flow counters.
func (a *Account) Stats() Stats {
	return Stats{TotalDeposited: a.totalIn, TotalWithdrawn: a.totalOut, DepositCount: a.depositCount}
}

// PendingEvents implements repo.Aggregate.
func (a *Account) PendingEvents() []eventstore.UncommittedEvent {
	return a.pending
}

// PullPending implements repo.Aggregate.
func (a *Account) PullPending() []eventstore.UncommittedEvent {
	out := a.pending
	a.pending = nil
	return out
}

// stateV1 is the serialized snapshot shape. Bumping this constant and adding
// an upgrade path is how aggregate-state snapshots evolve; an unrecognized
// state schema simply triggers full replay (see UnmarshalState).
const snapshotStateVersion = 1

type snapshotState struct {
	// Open is omitted from the wire: a snapshot exists only for an opened
	// account, so it always restores to open=true.
	Owner        string `json:"owner"`
	Currency     string `json:"currency"`
	AccountType  string `json:"account_type"`
	CreditLimit  int64  `json:"credit_limit"`
	Balance      int64  `json:"balance"`
	TotalIn      int64  `json:"total_in"`
	TotalOut     int64  `json:"total_out"`
	DepositCount int64  `json:"deposit_count"`
}

// MarshalState implements repo.Aggregate.
func (a *Account) MarshalState() (int, []byte, error) {
	data, err := json.Marshal(snapshotState{
		Owner:        a.owner,
		Currency:     a.currency,
		AccountType:  a.accountType,
		CreditLimit:  a.creditLimit,
		Balance:      a.balance,
		TotalIn:      a.totalIn,
		TotalOut:     a.totalOut,
		DepositCount: a.depositCount,
	})
	if err != nil {
		return 0, nil, fmt.Errorf("bank: marshal snapshot: %w", err)
	}
	return snapshotStateVersion, data, nil
}

// UnmarshalState implements repo.Aggregate. Unknown state schema => error so
// the repository falls back to full event replay.
func (a *Account) UnmarshalState(schemaVersion int, data []byte) error {
	if schemaVersion != snapshotStateVersion {
		return fmt.Errorf("bank: unsupported snapshot state schema v%d (understand v%d)", schemaVersion, snapshotStateVersion)
	}
	var s snapshotState
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("bank: unmarshal snapshot: %w", err)
	}
	a.open = true
	a.owner = s.Owner
	a.currency = s.Currency
	a.accountType = s.AccountType
	a.creditLimit = s.CreditLimit
	a.balance = s.Balance
	a.totalIn = s.TotalIn
	a.totalOut = s.TotalOut
	a.depositCount = s.DepositCount
	return nil
}

// --- Commands --------------------------------------------------------------

// Open opens the account and records bank.account.opened (v2).
func (a *Account) Open(owner, currency string, accountType string, creditLimit int64) error {
	if a.open {
		return ErrAlreadyOpen
	}
	if owner == "" {
		return errors.New("bank: owner required")
	}
	if currency == "" {
		return errors.New("bank: currency required")
	}
	if accountType == "" {
		accountType = DefaultAccountType
	}
	if creditLimit < 0 {
		return errors.New("bank: credit limit cannot be negative")
	}
	a.raise(TypeAccountOpened, SchemaV2, AccountOpenedV2{
		AccountID:   a.id,
		Owner:       owner,
		Currency:    currency,
		AccountType: accountType,
		CreditLimit: creditLimit,
	})
	// Reflect the decision in command-side state immediately, so subsequent
	// commands on the same instance are valid before Save. Save replays the
	// committed envelope; ApplyEvent treats re-applying the same pending
	// event idempotently (see apply* helpers).
	a.open = true
	a.owner = owner
	a.currency = currency
	a.accountType = accountType
	a.creditLimit = creditLimit
	return nil
}

// Deposit records bank.money.deposited (v2).
func (a *Account) Deposit(amount int64, note, channel, externalRef string) error {
	if !a.open {
		return ErrNotOpen
	}
	if amount <= 0 {
		return ErrInvalidAmount
	}
	if channel == "" {
		channel = DefaultChannel
	}
	a.raise(TypeMoneyDeposited, SchemaV2, MoneyDepositedV2{
		Amount:      amount,
		Note:        note,
		Channel:     channel,
		ExternalRef: externalRef,
	})
	a.balance += amount
	a.totalIn += amount
	a.depositCount++
	return nil
}

// Withdraw records bank.money.withdrawn (v2), honoring the credit limit.
func (a *Account) Withdraw(amount int64, note, channel, reasonCode string) error {
	if !a.open {
		return ErrNotOpen
	}
	if amount <= 0 {
		return ErrInvalidAmount
	}
	if channel == "" {
		channel = DefaultChannel
	}
	if reasonCode == "" {
		reasonCode = DefaultReasonCode
	}
	if a.balance+a.creditLimit < amount {
		return ErrInsufficientFunds
	}
	a.raise(TypeMoneyWithdrawn, SchemaV2, MoneyWithdrawnV2{
		Amount:     amount,
		Note:       note,
		Channel:    channel,
		ReasonCode: reasonCode,
	})
	a.balance -= amount
	a.totalOut += amount
	return nil
}

func (a *Account) raise(eventType string, schemaVersion int, payload any) {
	raw, err := json.Marshal(payload)
	if err != nil {
		// All payloads are plain structs; a marshal error is a bug.
		panic(fmt.Sprintf("bank: marshal %s: %v", eventType, err))
	}
	id := eventstore.NewEventID()
	a.pending = append(a.pending, eventstore.UncommittedEvent{
		EventID:       id,
		EventType:     eventType,
		SchemaVersion: schemaVersion,
		Payload:       raw,
	})
	a.pendingIDs[id] = struct{}{}
}

// ApplyEvent folds an upcast event into state.
func (a *Account) ApplyEvent(env eventstore.Envelope, decoded any) error {
	if env.Version != a.version+1 {
		return fmt.Errorf("bank: non-consecutive apply: at v%d got v%d", a.version, env.Version)
	}
	// Events produced by this instance are already reflected in command-side
	// state; Save folds the committed envelope only to advance the version.
	_, alreadyApplied := a.pendingIDs[env.EventID]
	if !alreadyApplied {
		switch p := decoded.(type) {
		case *AccountOpenedV2:
			if a.open {
				return ErrAlreadyOpen
			}
			a.open = true
			a.owner = p.Owner
			a.currency = p.Currency
			a.accountType = p.AccountType
			a.creditLimit = p.CreditLimit
		case *MoneyDepositedV2:
			if !a.open {
				return ErrNotOpen
			}
			a.balance += p.Amount
			a.totalIn += p.Amount
			a.depositCount++
		case *MoneyWithdrawnV2:
			if !a.open {
				return ErrNotOpen
			}
			a.balance -= p.Amount
			a.totalOut += p.Amount
		default:
			return fmt.Errorf("bank: unknown decoded payload %T for event %s", decoded, env.EventType)
		}
	}
	delete(a.pendingIDs, env.EventID)
	a.version = env.Version
	return nil
}

package bank

import (
	"errors"
	"fmt"

	"github.com/lechandonga/event-sourcing/projection"
)

// Account 是银行账户聚合根。
//
// 约定：Version 是已提交事件数（流内版本，从 0 起）；命令产生的新事件以
// 类型化形态暂存在 pending，由仓储在 Save 时统一通过事件注册表编码（盖当前
// schemaVersion）。落盘成功后仓储调用 CommitEvents 清空 pending 并对齐版本。
type Account struct {
	id       string
	holder   string
	currency string
	balance  int64 // 单位：分
	open     bool
	version  int64
	pending  []any // *AccountOpened | *MoneyDeposited | *MoneyWithdrawn
}

// AccountType 是聚合类型常量（流名前缀）。
const AccountType = "bank.Account"

var (
	// ErrAccountClosed 对未开户账户执行操作。
	ErrAccountClosed = errors.New("bank: account not open")
	// ErrInvalidAmount 金额非正。
	ErrInvalidAmount = errors.New("bank: amount must be positive")
	// ErrInsufficientFunds 余额不足。
	ErrInsufficientFunds = errors.New("bank: insufficient funds")
)

// OpenAccount 执行开户命令，返回携带一个待提交事件的新聚合。
func OpenAccount(id, holder string, initialCents int64, currency string) (*Account, error) {
	if id == "" || holder == "" {
		return nil, errors.New("bank: id and holder are required")
	}
	if initialCents < 0 {
		return nil, fmt.Errorf("%w: negative initial balance", ErrInvalidAmount)
	}
	if currency == "" {
		currency = DefaultCurrency
	}
	a := &Account{id: id, currency: currency}
	a.raise(&AccountOpened{
		Holder:              holder,
		InitialBalanceCents: initialCents,
		Currency:            currency,
	})
	return a, nil
}

func (a *Account) ID() string          { return a.id }
func (a *Account) Holder() string      { return a.holder }
func (a *Account) Currency() string    { return a.currency }
func (a *Account) BalanceCents() int64 { return a.balance }
func (a *Account) Version() int64      { return a.version }
func (a *Account) IsOpen() bool        { return a.open }

// Deposit 记录一笔存款。
func (a *Account) Deposit(cents int64, note string) error {
	if !a.open {
		return ErrAccountClosed
	}
	if cents <= 0 {
		return fmt.Errorf("%w: deposit %d", ErrInvalidAmount, cents)
	}
	a.raise(&MoneyDeposited{AmountCents: cents, Note: note})
	return nil
}

// Withdraw 记录一笔取款；余额不足在写侧直接拒绝，不会产生事件。
func (a *Account) Withdraw(cents int64, note string) error {
	if !a.open {
		return ErrAccountClosed
	}
	if cents <= 0 {
		return fmt.Errorf("%w: withdraw %d", ErrInvalidAmount, cents)
	}
	if cents > a.balance {
		return fmt.Errorf("%w: balance=%d requested=%d", ErrInsufficientFunds, a.balance, cents)
	}
	a.raise(&MoneyWithdrawn{AmountCents: cents, Note: note})
	return nil
}

// Apply 把一个已提交（并已升级到当前 schema）的历史事件投影到聚合状态。
// 仓储历史回放走这里；与命令路径共用 applyEvent，保证两条路径的状态转移一致。
func (a *Account) Apply(e projection.DecodedEvent) error {
	if a.id == "" {
		a.id = streamIDOf(e.Envelope.Stream)
	}
	if err := a.applyEvent(e.Event); err != nil {
		return err
	}
	a.version = e.Envelope.Version
	return nil
}

// raise 是命令路径：登记待提交事件，并立即把它应用到内存状态。
// 版本号只在仓储成功提交后由 CommitAccessor 推进——这样即使提交被乐观并发
// 拒绝，聚合也只是一个未提交的草稿（调用方应丢弃并重载）。
func (a *Account) raise(ev any) {
	if err := a.applyEvent(ev); err != nil {
		panic(err) // 当前命令构造的都是已知事件类型
	}
	a.pending = append(a.pending, ev)
}

// applyEvent 是纯状态转移（不触碰版本号、不依赖外部状态）。
func (a *Account) applyEvent(v any) error {
	switch ev := v.(type) {
	case *AccountOpened:
		a.holder = ev.Holder
		a.currency = ev.Currency
		a.balance = ev.InitialBalanceCents
		a.open = true
	case *MoneyDeposited:
		a.balance += ev.AmountCents
	case *MoneyWithdrawn:
		a.balance -= ev.AmountCents
	default:
		return fmt.Errorf("bank: unknown event %T", v)
	}
	return nil
}

// PendingEvents 返回未提交事件（仓储在 Save 时编码）。
func (a *Account) PendingEvents() []any {
	out := make([]any, len(a.pending))
	copy(out, a.pending)
	return out
}

// CommitEvents 在仓储落盘成功后调用：对齐已提交版本并清空 pending。
func (a *Account) CommitEvents(newVersion int64) { a.version = newVersion; a.pending = nil }

// AccountState 是快照负载。
type AccountState struct {
	ID       string `json:"id"`
	Holder   string `json:"holder"`
	Currency string `json:"currency"`
	Balance  int64  `json:"balanceCents"`
	Open     bool   `json:"open"`
}

// SnapshotState 导出当前状态用于打快照。
func (a *Account) SnapshotState() AccountState {
	return AccountState{
		ID:       a.id,
		Holder:   a.holder,
		Currency: a.currency,
		Balance:  a.balance,
		Open:     a.open,
	}
}

// RestoreState 用快照状态恢复聚合（剩余事件由仓储从 Version+1 继续回放）。
func (a *Account) RestoreState(s AccountState) {
	a.id, a.holder, a.currency = s.ID, s.Holder, s.Currency
	a.balance, a.open = s.Balance, s.Open
	a.pending = nil
}

// streamIDOf 从规范流名 "<type>-<id>" 提取 id 部分。
func streamIDOf(name string) string {
	for i := 0; i < len(name); i++ {
		if name[i] == '-' {
			return name[i+1:]
		}
	}
	return name
}

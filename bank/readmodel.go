package bank

import (
	"context"
	"fmt"
	"sync"

	"github.com/lechandonga/event-sourcing/projection"
)

// AccountSummary 是读模型中的一行账户汇总。
type AccountSummary struct {
	ID              string
	Holder          string
	Currency        string
	BalanceCents    int64
	DepositCount    int64
	WithdrawalCount int64
	DepositCountV1  int64 // 演示用：以 v1 schema 到达的存款数（升级前原始版本）
	OpenedVersion   int   // 事件被重放时识别到的原始 schema 版本
	LastSequence    int64
}

// SummaryReadModel 是账户汇总读模型。
//
// Apply 是确定性的：不依赖当前时间、随机数或外部状态；
// 重复/乱序事件由投影仪在进入 Apply 前去重排序，因此计数天然不会翻倍。
// 内部带互斥锁，重建与查询可以并发（查询会看到中间态，这是允许的——
// 重建不阻塞写入；如需隔离，调用方可重建到影子实例再原子替换）。
type SummaryReadModel struct {
	mu       sync.RWMutex
	accounts map[string]*AccountSummary
	// totalApplied 统计实际进入 Apply 的事件数（测试用于断言无重复计数）。
	totalApplied int64
}

// NewSummaryReadModel 创建空读模型。
func NewSummaryReadModel() *SummaryReadModel {
	return &SummaryReadModel{accounts: make(map[string]*AccountSummary)}
}

// Apply 处理单个事件。
func (m *SummaryReadModel) Apply(_ context.Context, e projection.DecodedEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applyLocked(e)
}

// ApplyBatch 批量处理（少加锁；事件已经过去重、排序、升级）。
func (m *SummaryReadModel) ApplyBatch(_ context.Context, events []projection.DecodedEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range events {
		if err := m.applyLocked(e); err != nil {
			return err
		}
	}
	return nil
}

func (m *SummaryReadModel) applyLocked(e projection.DecodedEvent) error {
	id := streamIDOf(e.Envelope.Stream)
	row := m.accounts[id]
	switch ev := e.Event.(type) {
	case *AccountOpened:
		if row != nil {
			// 同一账户的开户事件只会在全局序号唯一地出现一次；走到这里说明投递重复，
			// 幂等忽略而不是再建一行（投影仪通常已在前面去重，这里是第二道防线）。
			return nil
		}
		m.accounts[id] = &AccountSummary{
			ID:            id,
			Holder:        ev.Holder,
			Currency:      ev.Currency,
			BalanceCents:  ev.InitialBalanceCents,
			OpenedVersion: e.OriginalVersion,
			LastSequence:  e.Envelope.GlobalSequence,
		}
	case *MoneyDeposited:
		if row == nil {
			return fmt.Errorf("bank: deposit for unknown account %q (seq=%d)", id, e.Envelope.GlobalSequence)
		}
		row.BalanceCents += ev.AmountCents
		row.DepositCount++
		if e.OriginalVersion == 1 {
			row.DepositCountV1++
		}
	case *MoneyWithdrawn:
		if row == nil {
			return fmt.Errorf("bank: withdrawal for unknown account %q (seq=%d)", id, e.Envelope.GlobalSequence)
		}
		row.BalanceCents -= ev.AmountCents
		row.WithdrawalCount++
	default:
		return fmt.Errorf("bank: unsupported event %T", e.Event)
	}
	if row != nil {
		row.LastSequence = e.Envelope.GlobalSequence
	}
	m.totalApplied++
	return nil
}

// Reset 清空全部状态（重建前调用）。
func (m *SummaryReadModel) Reset(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.accounts = make(map[string]*AccountSummary)
	m.totalApplied = 0
	return nil
}

// Get 返回账户汇总副本；不存在返回 nil。
func (m *SummaryReadModel) Get(id string) *AccountSummary {
	m.mu.RLock()
	defer m.mu.RUnlock()
	row := m.accounts[id]
	if row == nil {
		return nil
	}
	cp := *row
	return &cp
}

// Snapshot 返回所有账户汇总副本（快照/对比用）。
func (m *SummaryReadModel) Snapshot() map[string]AccountSummary {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]AccountSummary, len(m.accounts))
	for id, row := range m.accounts {
		out[id] = *row
	}
	return out
}

// TotalApplied 返回进入 Apply 的事件总数。
func (m *SummaryReadModel) TotalApplied() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.totalApplied
}

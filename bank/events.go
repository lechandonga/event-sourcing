// Package bank 是示例银行域，演示事件结构演进：
//
//	AccountOpened
//	  v1: { holder, initialBalance }              金额单位：元（整数，废弃语义）
//	  v2: { holder, initialBalanceCents }          金额单位：分（v1 值 ×100）
//	  v3: { holder, initialBalanceCents, currency } 新增币种，缺省 "USD"
//
//	MoneyDeposited
//	  v1: { amount }                                金额单位：元
//	  v2: { amountCents }                           金额单位：分
//	  v3: { amountCents, note }                     新增备注，缺省 ""
//
//	MoneyWithdrawn
//	  v1: { amount }
//	  v2: { amountCents }
//	  v3: { amountCents, note }
//
// 所有升级器都是确定性纯函数；存储中的历史 JSON 永不被修改。
package bank

// AccountOpened 是当前 v3 结构。
type AccountOpened struct {
	Holder              string `json:"holder"`
	InitialBalanceCents int64  `json:"initialBalanceCents"`
	Currency            string `json:"currency"`
}

// EventType / EventSchemaVersion 实现注册表约定。
func (AccountOpened) EventType() string       { return "bank.AccountOpened" }
func (AccountOpened) EventSchemaVersion() int { return 3 }

// MoneyDeposited 是当前 v3 结构。
type MoneyDeposited struct {
	AmountCents int64  `json:"amountCents"`
	Note        string `json:"note"`
}

func (MoneyDeposited) EventType() string       { return "bank.MoneyDeposited" }
func (MoneyDeposited) EventSchemaVersion() int { return 3 }

// MoneyWithdrawn 是当前 v3 结构。
type MoneyWithdrawn struct {
	AmountCents int64  `json:"amountCents"`
	Note        string `json:"note"`
}

func (MoneyWithdrawn) EventType() string       { return "bank.MoneyWithdrawn" }
func (MoneyWithdrawn) EventSchemaVersion() int { return 3 }

// OpenUpcasters 返回 AccountOpened 的逐级升级链（key 为源版本）。
func OpenUpcasters() map[int]func(map[string]any) {
	return map[int]func(map[string]any){
		// v1 -> v2：金额单位由“元”改为“分”，并迁移字段名。
		1: func(m map[string]any) {
			yuan := eventpkgGetInt64(m, "initialBalance", 0)
			m["initialBalanceCents"] = yuan * 100
			delete(m, "initialBalance")
		},
		// v2 -> v3：新增币种字段；历史事件缺省为 USD（确定且稳定）。
		2: func(m map[string]any) {
			if _, ok := m["currency"]; !ok {
				m["currency"] = DefaultCurrency
			}
		},
	}
}

// MoneyUpcasters 同时适用于存款与取款（字段形状相同）。
func moneyUpcasters() map[int]func(map[string]any) {
	return map[int]func(map[string]any){
		1: func(m map[string]any) {
			yuan := eventpkgGetInt64(m, "amount", 0)
			m["amountCents"] = yuan * 100
			delete(m, "amount")
		},
		2: func(m map[string]any) {
			if _, ok := m["note"]; !ok {
				m["note"] = ""
			}
		},
	}
}

// DepositedUpcasters 返回 MoneyDeposited 的升级链。
func DepositedUpcasters() map[int]func(map[string]any) { return moneyUpcasters() }

// WithdrawnUpcasters 返回 MoneyWithdrawn 的升级链。
func WithdrawnUpcasters() map[int]func(map[string]any) { return moneyUpcasters() }

// DefaultCurrency 是历史事件升级时新增币种字段的稳定缺省值。
const DefaultCurrency = "USD"

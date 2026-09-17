package bank

import (
	"encoding/json"

	"github.com/lechandonga/event-sourcing/event"
)

// 历史结构仅用于构造测试夹具：模拟“当年以旧版本写入”的原始负载。
// 它们不会在生产代码中被解码——读取路径一律升级到 v3。

// AccountOpenedV1 是最初版本（金额单位：元）。
type AccountOpenedV1 struct {
	Holder         string `json:"holder"`
	InitialBalance int64  `json:"initialBalance"`
}

// MarshalV1 把 v1 夹具编码为【当年的原始 JSON】（显式 schemaVersion=1）。
func (e AccountOpenedV1) MarshalV1() json.RawMessage {
	return mustMarshal(map[string]any{
		"schemaVersion":  1,
		"holder":         e.Holder,
		"initialBalance": e.InitialBalance,
	})
}

// AccountOpenedV2 是第二版（分，暂无币种）。
type AccountOpenedV2 struct {
	Holder              string `json:"holder"`
	InitialBalanceCents int64  `json:"initialBalanceCents"`
}

func (e AccountOpenedV2) MarshalV2() json.RawMessage {
	return mustMarshal(map[string]any{
		"schemaVersion":       2,
		"holder":              e.Holder,
		"initialBalanceCents": e.InitialBalanceCents,
	})
}

// MoneyV1 是存取款事件的 v1 夹具（元）。
type MoneyV1 struct {
	Amount int64 `json:"amount"`
}

func (e MoneyV1) MarshalV1() json.RawMessage {
	return mustMarshal(map[string]any{"schemaVersion": 1, "amount": e.Amount})
}

// MoneyV2 是存取款事件的 v2 夹具（分，无备注）。
type MoneyV2 struct {
	AmountCents int64 `json:"amountCents"`
}

func (e MoneyV2) MarshalV2() json.RawMessage {
	return mustMarshal(map[string]any{"schemaVersion": 2, "amountCents": e.AmountCents})
}

// NewRegistry 构造注册了银行域全部事件及升级链的注册表。
func NewRegistry() *event.Registry {
	r := event.NewRegistry()
	r.MustRegister((*AccountOpened)(nil), toEventUpcasters(OpenUpcasters()))
	r.MustRegister((*MoneyDeposited)(nil), toEventUpcasters(DepositedUpcasters()))
	r.MustRegister((*MoneyWithdrawn)(nil), toEventUpcasters(WithdrawnUpcasters()))
	return r
}

func mustMarshal(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return raw
}

package bank_test

import (
	"encoding/json"
	"testing"

	"github.com/lechandonga/event-sourcing/bank"
	"github.com/lechandonga/event-sourcing/event"
)

func currentV3(t *testing.T, v event.Versioned) json.RawMessage {
	t.Helper()
	raw, err := event.Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func jsonUnmarshal(raw []byte, v any) error { return json.Unmarshal(raw, v) }

var _ = bank.DefaultCurrency

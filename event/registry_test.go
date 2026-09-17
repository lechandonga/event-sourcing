package event_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/lechandonga/event-sourcing/event"
)

type v1Thing struct {
	A string `json:"a"`
}
type v2Thing struct {
	A string `json:"a"`
	B string `json:"b"`
}
type v3Thing struct {
	A string `json:"a"`
	B string `json:"b"`
	C int    `json:"c"`
}

func newTestRegistry(t *testing.T) *event.Registry {
	t.Helper()
	r := event.NewRegistry()
	r.Register("thing", 3, func(raw json.RawMessage) (any, error) {
		var v v3Thing
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		return &v, nil
	})
	r.AddUpcaster("thing", 1, func(raw json.RawMessage) (json.RawMessage, error) {
		m := map[string]any{}
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, err
		}
		if _, ok := m["b"]; !ok {
			m["b"] = "default-b"
		}
		return json.Marshal(m)
	})
	r.AddUpcaster("thing", 2, func(raw json.RawMessage) (json.RawMessage, error) {
		m := map[string]any{}
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, err
		}
		if _, ok := m["c"]; !ok {
			m["c"] = 42
		}
		return json.Marshal(m)
	})
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestUpcastChainFillsDefaultsAcrossVersions(t *testing.T) {
	r := newTestRegistry(t)
	// v1 payload: no b, no c.
	out, err := r.Decode("thing", 1, json.RawMessage(`{"a":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	v := out.(*v3Thing)
	if v.A != "x" || v.B != "default-b" || v.C != 42 {
		t.Fatalf("upcast result wrong: %+v", v)
	}
}

func TestUpcastIsDeterministicAndPreservesExplicitValues(t *testing.T) {
	r := newTestRegistry(t)
	raw := json.RawMessage(`{"a":"x","b":"explicit","unknown":"kept"}`)
	first, err := r.Upcast("thing", 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	second, err := r.Upcast("thing", 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatalf("upcast non-deterministic:\n%s\n%s", first, second)
	}
	if !strings.Contains(string(first), `"b":"explicit"`) {
		t.Fatalf("explicit value overwritten: %s", first)
	}
	if !strings.Contains(string(first), `"unknown":"kept"`) {
		t.Fatalf("unknown field dropped: %s", first)
	}
	// Original bytes untouched.
	if string(raw) != `{"a":"x","b":"explicit","unknown":"kept"}` {
		t.Fatalf("input modified: %s", raw)
	}
}

func TestUpcastCurrentVersionIsIdentity(t *testing.T) {
	r := newTestRegistry(t)
	raw := json.RawMessage(`{"a":"x","b":"y","c":7}`)
	out, err := r.Upcast("thing", 3, raw)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != string(raw) {
		t.Fatalf("identity upcast changed bytes: %s", out)
	}
}

func TestNewerSchemaRejected(t *testing.T) {
	r := newTestRegistry(t)
	_, err := r.Decode("thing", 9, json.RawMessage(`{}`))
	var ns *event.NewerSchemaError
	if !errors.As(err, &ns) {
		t.Fatalf("want NewerSchemaError, got %v", err)
	}
}

func TestUnknownEventTypeUpcastFails(t *testing.T) {
	r := newTestRegistry(t)
	raw := json.RawMessage(`{"z":1}`)
	out, err := r.Upcast("mystery", 1, raw)
	var ue *event.UnknownEventError
	if !errors.As(err, &ue) {
		t.Fatalf("want UnknownEventError, got %v", err)
	}
	if string(out) != "" {
		t.Fatalf("expected no output on unknown error, got %s", out)
	}
}

func TestValidateDetectsMissingChain(t *testing.T) {
	r := event.NewRegistry()
	r.Register("broken", 3, func(raw json.RawMessage) (any, error) { return nil, nil })
	err := r.Validate()
	if err == nil || !strings.Contains(err.Error(), "missing upcaster") {
		t.Fatalf("want missing upcaster error, got %v", err)
	}
}

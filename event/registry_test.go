package event_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/lechandonga/event-sourcing/event"
)

type evtV1 struct {
	Name string `json:"name"`
}
type evtV2 struct {
	Name    string `json:"name"`
	Comment string `json:"comment"` // 新增字段，缺省 "none"
}
type evtV3 struct {
	Name    string `json:"name"`
	Comment string `json:"comment"`
	Legacy  int64  `json:"legacy"` // 语义迁移：v2 的 points 乘 2
	Points  int64  `json:"points"`
}

func (*evtV3) EventType() string       { return "test.Evt" }
func (*evtV3) EventSchemaVersion() int { return 3 }

func newTestRegistry(t *testing.T) *event.Registry {
	t.Helper()
	r := event.NewRegistry()
	err := r.Register((*evtV3)(nil), map[int]event.Upcaster{
		1: func(m map[string]any) {
			// 新增字段缺省值必须确定、稳定
			m["comment"] = "none"
			// 保留 v1 的 points（若有）
		},
		2: func(m map[string]any) {
			points := event.GetInt64(m, "points", 0)
			m["legacy"] = points * 2
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestUpcastChainFromV1(t *testing.T) {
	r := newTestRegistry(t)
	v1 := json.RawMessage(`{"schemaVersion":1,"name":"a","points":5}`)
	v, from, err := r.Decode("test.Evt", v1)
	if err != nil {
		t.Fatal(err)
	}
	if from != 1 {
		t.Fatalf("original version = %d, want 1", from)
	}
	got := v.(*evtV3)
	if got.Name != "a" || got.Comment != "none" || got.Points != 5 || got.Legacy != 10 {
		t.Fatalf("upcast result wrong: %+v", got)
	}
	// 历史负载必须原封不动（不改写历史）
	if string(v1) != `{"schemaVersion":1,"name":"a","points":5}` {
		t.Fatalf("raw history payload was modified: %s", v1)
	}
}

func TestUpcastFromV2AndCurrent(t *testing.T) {
	r := newTestRegistry(t)
	// v2：缺省字段在 v2->v3 不补（comment 在 1->2 补）；v2 事件本就有 comment
	v2 := json.RawMessage(`{"schemaVersion":2,"name":"b","comment":"hi","points":3}`)
	v, from, _ := r.Decode("test.Evt", v2)
	got := v.(*evtV3)
	if from != 2 || got.Comment != "hi" || got.Legacy != 6 {
		t.Fatalf("v2 upcast wrong: %+v from=%d", got, from)
	}
	// 无 schemaVersion 视为当前版本；未知字段（pointsEx）被自然忽略，不报错
	noVer := json.RawMessage(`{"name":"c","comment":"z","points":1,"pointsEx":99}`)
	v, from, err := r.Decode("test.Evt", noVer)
	if err != nil {
		t.Fatal(err)
	}
	if from != 3 {
		t.Fatalf("missing version should mean current, got %d", from)
	}
	if v.(*evtV3).Name != "c" {
		t.Fatal("unknown field must be ignored, not fatal")
	}
}

func TestUpcastIsDeterministic(t *testing.T) {
	r := newTestRegistry(t)
	raw := json.RawMessage(`{"schemaVersion":1,"name":"d","points":7}`)
	first, _, err := r.Decode("test.Evt", raw)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		again, _, err := r.Decode("test.Evt", raw)
		if err != nil {
			t.Fatal(err)
		}
		a, _ := json.Marshal(first)
		b, _ := json.Marshal(again)
		if string(a) != string(b) {
			t.Fatalf("upcast not deterministic: %s vs %s", a, b)
		}
	}
}

func TestDecodeErrors(t *testing.T) {
	r := newTestRegistry(t)
	if _, _, err := r.Decode("nope.Type", json.RawMessage(`{}`)); !errors.Is(err, event.ErrUnknownEventType) {
		t.Fatalf("want unknown event type, got %v", err)
	}
	// 未来版本（比注册表新）必须拒绝而不是乱猜
	if _, _, err := r.Decode("test.Evt", json.RawMessage(`{"schemaVersion":99}`)); !errors.Is(err, event.ErrBadSchemaVersion) {
		t.Fatalf("want bad schema version, got %v", err)
	}
	// 缺失升级器：只注册 1->3 的跳级链是不允许的，下面验证错误信息
	r2 := event.NewRegistry()
	err := r2.Register((*evtV3)(nil), map[int]event.Upcaster{
		1: func(m map[string]any) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = r2.Decode("test.Evt", json.RawMessage(`{"schemaVersion":1}`))
	if err == nil || !errors.Is(err, event.ErrBadSchemaVersion) || !strings.Contains(err.Error(), "missing upcaster") {
		t.Fatalf("want missing-upcaster error, got %v", err)
	}
}

func TestEncodeStampsCurrentVersion(t *testing.T) {
	raw, err := event.Encode(&evtV3{Name: "x"})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m["schemaVersion"] != float64(3) {
		t.Fatalf("encoded payload should carry schemaVersion=3, got %v", m["schemaVersion"])
	}
}

func TestDoubleRegisterRejected(t *testing.T) {
	r := event.NewRegistry()
	if err := r.Register((*evtV3)(nil), nil); err != nil {
		t.Fatal(err)
	}
	if err := r.Register((*evtV3)(nil), nil); err == nil {
		t.Fatal("double registration must fail")
	}
}

package snapshot_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/lechandonga/event-sourcing/snapshot"
)

func stores(t *testing.T) map[string]snapshot.Store {
	t.Helper()
	mem := snapshot.NewMemoryStore()
	t.Cleanup(func() { _ = mem.Close(context.Background()) })
	fil := snapshot.NewFileStore(t.TempDir())
	t.Cleanup(func() { _ = fil.Close(context.Background()) })
	return map[string]snapshot.Store{"memory": mem, "file": fil}
}

func sample(ver int64) *snapshot.Snapshot {
	return &snapshot.Snapshot{
		StreamType:     "bank.Account",
		StreamID:       "acct-1",
		Version:        ver,
		GlobalSequence: ver * 2,
		Payload:        json.RawMessage(`{"balanceCents":42}`),
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	ctx := context.Background()
	for name, st := range stores(t) {
		t.Run(name, func(t *testing.T) {
			s := sample(7)
			if err := s.Seal(); err != nil {
				t.Fatal(err)
			}
			if err := st.Save(ctx, s); err != nil {
				t.Fatal(err)
			}
			got, err := st.Load(ctx, "bank.Account", "acct-1")
			if err != nil {
				t.Fatal(err)
			}
			if got.Version != 7 || got.GlobalSequence != 14 {
				t.Fatalf("snapshot header mismatch: %+v", got)
			}
			var payload map[string]any
			if err := json.Unmarshal(got.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if payload["balanceCents"] != float64(42) {
				t.Fatalf("snapshot payload mismatch: %s", got.Payload)
			}
		})
	}
}

func TestMissingSnapshotIsNotCorrupt(t *testing.T) {
	ctx := context.Background()
	for name, st := range stores(t) {
		t.Run(name, func(t *testing.T) {
			_, err := st.Load(ctx, "bank.Account", "missing")
			if !errors.Is(err, snapshot.ErrSnapshotNotFound) {
				t.Fatalf("missing snapshot -> ErrSnapshotNotFound, got %v", err)
			}
		})
	}
}

func TestCorruptPayloadDetected(t *testing.T) {
	ctx := context.Background()
	for name, st := range stores(t) {
		t.Run(name, func(t *testing.T) {
			s := sample(5)
			if err := s.Seal(); err != nil {
				t.Fatal(err)
			}
			if err := st.Save(ctx, s); err != nil {
				t.Fatal(err)
			}
			// 篡改一个字节（平衡值），校验和必须失配
			switch x := st.(type) {
			case *snapshot.MemoryStore:
				// 内存存储无法外部篡改；通过校验和逻辑的单测覆盖（见 TestChecksum）。
				_ = x
			case *snapshot.FileStore:
				corruptViaFile(t, st, `{"balanceCents":999}`)
			}
			if _, err := st.Load(ctx, "bank.Account", "acct-1"); err != nil {
				if !errors.Is(err, snapshot.ErrSnapshotCorrupt) {
					t.Fatalf("tampered snapshot must be ErrSnapshotCorrupt, got %v", err)
				}
			} else if name == "file" {
				t.Fatal("expected corruption error after tampering")
			}
			// 损坏后 Delete 必须能清理，使调用方安全回退
			if err := st.Delete(ctx, "bank.Account", "acct-1"); err != nil {
				t.Fatal(err)
			}
			if _, err := st.Load(ctx, "bank.Account", "acct-1"); !errors.Is(err, snapshot.ErrSnapshotNotFound) {
				t.Fatalf("after delete want not-found, got %v", err)
			}
		})
	}
}

func TestChecksumCoversFields(t *testing.T) {
	s := sample(1)
	if err := s.Seal(); err != nil {
		t.Fatal(err)
	}
	clone := *s
	clone.Version = 2 // 改字段不改 checksum
	if err := clone.Verify(); !errors.Is(err, snapshot.ErrSnapshotCorrupt) {
		t.Fatalf("field tamper must fail verify, got %v", err)
	}
	clone2 := *s
	clone2.Payload = json.RawMessage(`{"balanceCents":43}`)
	if err := clone2.Verify(); !errors.Is(err, snapshot.ErrSnapshotCorrupt) {
		t.Fatalf("payload tamper must fail verify, got %v", err)
	}
}

func TestSaveRequiresSeal(t *testing.T) {
	ctx := context.Background()
	st := snapshot.NewMemoryStore()
	s := sample(1) // 未 Seal
	if err := st.Save(ctx, s); !errors.Is(err, snapshot.ErrSnapshotCorrupt) {
		t.Fatalf("unsealed snapshot must be rejected, got %v", err)
	}
}

// corruptViaFile 通过文件存储的公开路径直接覆写 payload 字段，模拟磁盘损坏。
func corruptViaFile(t *testing.T, st snapshot.Store, payload string) {
	t.Helper()
	fs := st.(*snapshot.FileStore)
	p := filepath.Join(fs.DirForTest(), "bank.Account", "acct-1.snapshot.json")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	doc["payload"] = json.RawMessage(payload)
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, out, 0o644); err != nil {
		t.Fatal(err)
	}
}

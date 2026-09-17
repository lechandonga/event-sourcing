package eventstore_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/lechandonga/event-sourcing/eventstore"
)

func newTestEvent(typ string, n int) eventstore.UncommittedEvent {
	return eventstore.UncommittedEvent{Type: typ, Data: []byte(fmt.Sprintf(`{"n":%d}`, n))}
}

func stores(t *testing.T) map[string]eventstore.Store {
	t.Helper()
	mem := eventstore.NewMemoryStore()
	t.Cleanup(func() { _ = mem.Close(context.Background()) })

	dir := t.TempDir()
	fs, err := eventstore.OpenFileStore(context.Background(), dir+"/events.log")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fs.Close(context.Background()) })

	return map[string]eventstore.Store{"memory": mem, "file": fs}
}

func TestAppendAndRead(t *testing.T) {
	ctx := context.Background()
	for name, st := range stores(t) {
		t.Run(name, func(t *testing.T) {
			sid := eventstore.StreamID{Type: "order", ID: "o1"}
			c1, err := st.Append(ctx, sid, eventstore.ExpectedVersionNew,
				[]eventstore.UncommittedEvent{newTestEvent("order.Created", 1)})
			if err != nil {
				t.Fatal(err)
			}
			if c1[0].Version != 1 || c1[0].GlobalSequence != 1 {
				t.Fatalf("unexpected ids: %+v", c1[0])
			}
			c2, err := st.Append(ctx, sid, eventstore.ExpectedVersion(1),
				[]eventstore.UncommittedEvent{
					newTestEvent("order.Paid", 2),
					newTestEvent("order.Shipped", 3),
				})
			if err != nil {
				t.Fatal(err)
			}
			if c2[0].Version != 2 || c2[1].Version != 3 || c2[1].GlobalSequence != 3 {
				t.Fatalf("batch versions wrong: %+v", c2)
			}
			got, err := st.ReadStream(ctx, sid, 1, 0)
			if err != nil || len(got) != 2 || got[0].Type != "order.Paid" {
				t.Fatalf("read stream: %v %+v", err, got)
			}
			all, _ := st.ReadAll(ctx, 0, 0)
			if len(all) != 3 || all[2].Type != "order.Shipped" {
				t.Fatalf("read all wrong: %+v", all)
			}
			// 区间读取
			mid, _ := st.ReadAll(ctx, 1, 2)
			if len(mid) != 1 || mid[0].Type != "order.Paid" {
				t.Fatalf("bounded readall wrong: %+v", mid)
			}
		})
	}
}

func TestConflictReasons(t *testing.T) {
	ctx := context.Background()
	for name, st := range stores(t) {
		t.Run(name, func(t *testing.T) {
			sid := eventstore.StreamID{Type: "order", ID: "c1"}

			// 期望新建，但流已存在
			if _, err := st.Append(ctx, sid, eventstore.ExpectedVersionNew,
				[]eventstore.UncommittedEvent{newTestEvent("order.Created", 1)}); err != nil {
				t.Fatal(err)
			}
			_, err := st.Append(ctx, sid, eventstore.ExpectedVersionNew,
				[]eventstore.UncommittedEvent{newTestEvent("order.Created", 9)})
			var ce *eventstore.ConflictError
			if !errors.As(err, &ce) || ce.Reason != eventstore.ReasonStreamExists {
				t.Fatalf("want stream-exists conflict, got %v", err)
			}
			if !errors.Is(err, eventstore.ErrVersionConflict) {
				t.Fatalf("conflict must satisfy errors.Is ErrVersionConflict: %v", err)
			}

			// 过期版本
			_, err = st.Append(ctx, sid, eventstore.ExpectedVersion(1),
				[]eventstore.UncommittedEvent{newTestEvent("order.Paid", 2)})
			if err != nil {
				t.Fatalf("first append at v1 should succeed: %v", err)
			}
			_, err = st.Append(ctx, sid, eventstore.ExpectedVersion(1),
				[]eventstore.UncommittedEvent{newTestEvent("order.Late", 3)})
			if !errors.As(err, &ce) || ce.Reason != eventstore.ReasonVersionConflict {
				t.Fatalf("want version-conflict, got %v", err)
			}
			if ce.Expected != 1 || ce.Actual != 2 {
				t.Fatalf("conflict detail wrong: %+v", ce)
			}

			// 流不存在却声明版本
			ghost := eventstore.StreamID{Type: "order", ID: "ghost"}
			_, err = st.Append(ctx, ghost, eventstore.ExpectedVersion(5),
				[]eventstore.UncommittedEvent{newTestEvent("order.X", 1)})
			if !errors.As(err, &ce) || ce.Reason != eventstore.ReasonStreamNotFound {
				t.Fatalf("want stream-not-found, got %v", err)
			}

			// 被拒绝的批次绝不入流：流里仍应只有 2 个事件
			got, _ := st.ReadStream(ctx, sid, 0, 0)
			if len(got) != 2 {
				t.Fatalf("rejected writes leaked into committed stream: %d events: %+v", len(got), got)
			}
		})
	}
}

func TestRejectsInvalidBatchesAtomically(t *testing.T) {
	ctx := context.Background()
	st := eventstore.NewMemoryStore()
	sid := eventstore.StreamID{Type: "x", ID: "1"}
	if _, err := st.Append(ctx, sid, eventstore.ExpectedVersionNew, nil); !errors.Is(err, eventstore.ErrEmptyBatch) {
		t.Fatalf("want empty batch, got %v", err)
	}
	if _, err := st.Append(ctx, sid, eventstore.ExpectedVersionNew,
		[]eventstore.UncommittedEvent{{Type: "", Data: []byte(`{}`)}}); !errors.Is(err, eventstore.ErrInvalidEvent) {
		t.Fatalf("want invalid event, got %v", err)
	}
	if _, err := st.StreamVersion(ctx, sid); !errors.Is(err, eventstore.ErrStreamNotFound) {
		t.Fatalf("stream must not exist after rejected batches, got %v", err)
	}
}

// TestConcurrentWritersSerializesByVersion 是核心并发场景：
// N 个 goroutine 基于各自观察到的版本写同一聚合，使用 CAS 重试，
// 最终 1..K 的每个版本必须恰好出现一次，无空洞、无重复。
func TestConcurrentWritersSerializesByVersion(t *testing.T) {
	ctx := context.Background()
	for name, st := range stores(t) {
		t.Run(name, func(t *testing.T) {
			sid := eventstore.StreamID{Type: "wallet", ID: "hot"}
			const writers = 16
			const perWriter = 25
			var wg sync.WaitGroup
			var conflicts int64
			var mu sync.Mutex
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < perWriter; i++ {
						for {
							v, err := st.StreamVersion(ctx, sid)
							if errors.Is(err, eventstore.ErrStreamNotFound) {
								v = 0
							} else if err != nil {
								t.Error(err)
								return
							}
							_, err = st.Append(ctx, sid, eventstore.ExpectedVersion(v),
								[]eventstore.UncommittedEvent{newTestEvent("wallet.Tx", i)})
							if err == nil {
								break
							}
							if errors.Is(err, eventstore.ErrVersionConflict) {
								mu.Lock()
								conflicts++
								mu.Unlock()
								continue
							}
							t.Error(err)
							return
						}
					}
				}()
			}
			wg.Wait()
			got, err := st.ReadStream(ctx, sid, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			want := int64(writers * perWriter)
			if int64(len(got)) != want {
				t.Fatalf("committed %d events, want %d (conflicts=%d)", len(got), want, conflicts)
			}
			for i, e := range got {
				if e.Version != int64(i+1) {
					t.Fatalf("version gap/dup at %d: got %d", i, e.Version)
				}
			}
			all, _ := st.ReadAll(ctx, 0, 0)
			for i, e := range all {
				if e.GlobalSequence != int64(i+1) {
					t.Fatalf("global sequence gap at %d: %d", i, e.GlobalSequence)
				}
			}
			if conflicts == 0 {
				t.Fatalf("expected at least some contention, got zero conflicts")
			}
		})
	}
}

func TestSubscriptionWakeup(t *testing.T) {
	ctx := context.Background()
	st := eventstore.NewMemoryStore()
	sid := eventstore.StreamID{Type: "x", ID: "1"}
	sub := st.Subscribe(0)
	defer sub.Close()
	if _, err := st.Append(ctx, sid, eventstore.ExpectedVersionNew,
		[]eventstore.UncommittedEvent{newTestEvent("x.E", 1)}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sub.C():
	case <-timeAfter():
		t.Fatal("subscriber not woken")
	}
	// 通知是“至少一次”的唤醒信号，数据以 ReadAll 为准
	all, _ := st.ReadAll(ctx, 0, 0)
	if len(all) != 1 {
		t.Fatalf("want 1 event, got %d", len(all))
	}
}

func TestFileStorePersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/nested/events.log"
	st, err := eventstore.OpenFileStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	sid := eventstore.StreamID{Type: "x", ID: "1"}
	for i := 1; i <= 3; i++ {
		exp := eventstore.ExpectedVersion(i - 1)
		if _, err := st.Append(ctx, sid, exp,
			[]eventstore.UncommittedEvent{newTestEvent("x.E", i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(ctx); err != nil {
		t.Fatal(err)
	}
	st2, err := eventstore.OpenFileStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close(ctx)
	if v, _ := st2.LastSequence(ctx); v != 3 {
		t.Fatalf("last seq after reopen = %d, want 3", v)
	}
	if _, err := st2.Append(ctx, sid, eventstore.ExpectedVersion(3),
		[]eventstore.UncommittedEvent{newTestEvent("x.E", 4)}); err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
}

// TestFileStoreTruncatesPartialLine 模拟崩溃留下的半写行：
// 重启后必须截断它、只恢复完整行，并能继续追加。
func TestFileStoreTruncatesPartialLine(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/events.log"
	st, err := eventstore.OpenFileStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	sid := eventstore.StreamID{Type: "x", ID: "1"}
	if _, err := st.Append(ctx, sid, eventstore.ExpectedVersionNew,
		[]eventstore.UncommittedEvent{newTestEvent("x.E", 1)}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// 手工追加一段不带换行符的垃圾（半写批次）。
	f, err := openAppend(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`[{"globalSequence":2`); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	st2, err := eventstore.OpenFileStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close(ctx)
	if v, _ := st2.LastSequence(ctx); v != 1 {
		t.Fatalf("partial line must be truncated, last seq=%d", v)
	}
	if _, err := st2.Append(ctx, sid, eventstore.ExpectedVersion(1),
		[]eventstore.UncommittedEvent{newTestEvent("x.E", 2)}); err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
}

// TestFileStoreRejectsCorruptHistory 确保序号/版本空洞被识别为损坏，
// 而不是静默接受（防止“跳过事件”的错误恢复）。
func TestFileStoreRejectsCorruptHistory(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/events.log"
	if err := writeAll(path, []byte("ES-EVENTS-V1\n")); err != nil {
		t.Fatal(err)
	}
	bad := `[{"globalSequence":5,"stream":"x-1","streamType":"x","version":1,"id":"i","type":"x.E","time":"2026-01-01T00:00:00Z","data":{"n":1}}]` + "\n"
	if err := appendAll(path, []byte(bad)); err != nil {
		t.Fatal(err)
	}
	if _, err := eventstore.OpenFileStore(ctx, path); !errors.Is(err, eventstore.ErrCorrupt) {
		t.Fatalf("want ErrCorrupt for gappy history, got %v", err)
	}
}

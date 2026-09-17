package eventstore

import (
	"context"
	"sync"
	"testing"
)

// 模拟 FileStore 的两阶段窗口，但故意不加 wmu 串行化：
// commitForDisk 的复检必须拒绝所有漂移提交，最终事件数与版本集合必须精确。
func TestTwoPhaseCommitRevalidation(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	st := StreamID{Type: "T", ID: "x"}
	batch := []UncommittedEvent{{Type: "E", Data: []byte(`{}`)}}

	var (
		wg       sync.WaitGroup
		ok       int64
		conflict int64
		mu       sync.Mutex
	)
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				p, err := s.prepareForDisk(st, ExpectedVersionAny, batch)
				if err != nil {
					t.Error(err)
					return
				}
				if _, err := s.commitForDisk(st, p); err != nil {
					mu.Lock()
					conflict++
					mu.Unlock()
					continue
				}
				mu.Lock()
				ok++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	all, _ := s.ReadAll(ctx, 0, 0)
	if int64(len(all)) != ok {
		t.Fatalf("committed=%d but readable=%d", ok, len(all))
	}
	verSeen := map[int64]bool{}
	for _, e := range all {
		if e.GlobalSequence != int64(len(verSeen))+1 {
			t.Fatalf("global sequence not contiguous: %d (have %d)", e.GlobalSequence, len(verSeen))
		}
		if e.Version <= 0 || e.Version > int64(len(all)) || verSeen[e.Version] {
			t.Fatalf("bad/dup version %d (len=%d)", e.Version, len(all))
		}
		verSeen[e.Version] = true
	}
	if ok+conflict != 10000 {
		t.Fatalf("attempts accounted=%d want 10000", ok+conflict)
	}
	t.Logf("committed=%d rejected=%d", ok, conflict)
}

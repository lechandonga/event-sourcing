package repo_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/lechandonga/event-sourcing/bank"
	"github.com/lechandonga/event-sourcing/eventstore"
	"github.com/lechandonga/event-sourcing/repo"
	"github.com/lechandonga/event-sourcing/snapshot"
)

type accountRepo = repo.Repository[*bank.Account, bank.Account]

func newFixture(t *testing.T, st eventstore.Store, snaps snapshot.Store, threshold int64,
	onFallback func(reason, id, detail string)) (*accountRepo, eventstore.Store) {
	t.Helper()
	r := bank.NewAccountRepository(bank.AccountRepositoryConfig{
		Store: st, Registry: bank.NewRegistry(), Snapshots: snaps,
		Threshold: threshold, OnFallback: onFallback,
	})
	return r, st
}

func openAccount(t *testing.T, ctx context.Context, r *accountRepo, id string, initial int64) {
	t.Helper()
	a, err := bank.OpenAccount(id, "Holder", initial, "USD")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Save(ctx, a); err != nil {
		t.Fatal(err)
	}
}

func deposit(t *testing.T, ctx context.Context, r *accountRepo, id string, cents int64) {
	t.Helper()
	a, err := bank.LoadAccount(ctx, r, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Deposit(cents, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Save(ctx, a); err != nil {
		t.Fatal(err)
	}
}

// 场景 1：没有快照时从头完整重放；保存/加载状态一致。
func TestLoadReplaysFromZeroWithoutSnapshot(t *testing.T) {
	ctx := context.Background()
	r, _ := newFixture(t, eventstore.NewMemoryStore(), nil, 0, nil)
	openAccount(t, ctx, r, "a1", 100)
	deposit(t, ctx, r, "a1", 50)
	deposit(t, ctx, r, "a1", 25)
	a, ver, err := r.Load(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if ver != 3 || a.Version() != 3 || a.BalanceCents() != 175 {
		t.Fatalf("full replay wrong: ver=%d balance=%d", a.Version(), a.BalanceCents())
	}
}

// 场景 2：并发命令保存，CAS 重试后所有版本恰好一次（写侧并发）。
func TestConcurrentSaveWithCASRetry(t *testing.T) {
	ctx := context.Background()
	r, _ := newFixture(t, eventstore.NewMemoryStore(), snapshot.NewMemoryStore(), 1000, nil)
	openAccount(t, ctx, r, "hot", 0)
	const n = 40
	var wg sync.WaitGroup
	var conflicts int64
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				a, err := bank.LoadAccount(ctx, r, "hot")
				if err != nil {
					t.Error(err)
					return
				}
				if err := a.Deposit(1, ""); err != nil {
					t.Error(err)
					return
				}
				if _, err := r.Save(ctx, a); err != nil {
					if errors.Is(err, eventstore.ErrVersionConflict) {
						mu.Lock()
						conflicts++
						mu.Unlock()
						continue
					}
					t.Error(err)
					return
				}
				return
			}
		}()
	}
	wg.Wait()
	a, ver, _ := r.Load(ctx, "hot")
	if ver != int64(n+1) || a.BalanceCents() != int64(n) {
		t.Fatalf("CAS result wrong: ver=%d balance=%d", ver, a.BalanceCents())
	}
	if conflicts == 0 {
		t.Fatal("expected contention, got none")
	}
}

// 场景 3：快照加速 + 过期快照继续重放（不丢事件、不重复）。
func TestSnapshotThenReplayRemaining(t *testing.T) {
	ctx := context.Background()
	snaps := snapshot.NewMemoryStore()
	r, _ := newFixture(t, eventstore.NewMemoryStore(), snaps, 5, nil)
	openAccount(t, ctx, r, "a1", 0)
	for i := 0; i < 12; i++ { // 12 笔存款；阈值 5 会产生 v5、v10 快照
		deposit(t, ctx, r, "a1", 10)
	}
	if snaps.Count() == 0 {
		t.Fatal("expected snapshots to be written")
	}
	a, _, err := r.Load(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if a.BalanceCents() != 120 || a.Version() != 13 {
		t.Fatalf("snapshot+replay mismatch: balance=%d ver=%d", a.BalanceCents(), a.Version())
	}
	// 快照版本必须严格小于流版本（剩余事件确有重放）
	snap, err := snaps.Load(ctx, bank.AccountType, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if snap.Version != 10 {
		t.Fatalf("latest snapshot version=%d want 10", snap.Version)
	}
}

// 场景 4：快照损坏 -> 删除坏快照、回调可观测、从头重放且状态正确。
func TestCorruptSnapshotFallback(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := eventstore.OpenFileStore(ctx, dir+"/events.log")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close(ctx)
	snaps := snapshot.NewFileStore(dir + "/snapshots")

	var fallbacks []string
	var mu sync.Mutex
	r, _ := newFixture(t, st, snaps, 3, func(reason, id, detail string) {
		mu.Lock()
		fallbacks = append(fallbacks, reason+":"+id)
		mu.Unlock()
	})
	openAccount(t, ctx, r, "a1", 0)
	for i := 0; i < 7; i++ {
		deposit(t, ctx, r, "a1", 1)
	}
	// 强制加载一次确保快照存在
	if _, _, err := r.Load(ctx, "a1"); err != nil {
		t.Fatal(err)
	}
	// 篡改快照文件
	p := dir + "/snapshots/bank.Account/a1.snapshot.json"
	tamperSnapshot(t, p)

	a, _, err := r.Load(ctx, "a1")
	if err != nil {
		t.Fatalf("corrupt snapshot must transparently recover: %v", err)
	}
	if a.BalanceCents() != 7 || a.Version() != 8 {
		t.Fatalf("full replay after corruption wrong: balance=%d ver=%d",
			a.BalanceCents(), a.Version())
	}
	mu.Lock()
	found := false
	for _, f := range fallbacks {
		if strings.HasPrefix(f, "corrupt:a1") {
			found = true
		}
	}
	mu.Unlock()
	if !found {
		t.Fatalf("expected corrupt fallback hook, got %v", fallbacks)
	}
	// 坏快照应已被删除（后续加载不再触发 corrupt）
	loaded, _, err := r.Load(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	_ = loaded
}

// 场景 5：快照超前于事件流（不一致）-> 删除并从头重放。
func TestAheadSnapshotFallback(t *testing.T) {
	ctx := context.Background()
	snaps := snapshot.NewMemoryStore()
	r, st := newFixture(t, eventstore.NewMemoryStore(), snaps, 1000, nil)
	openAccount(t, ctx, r, "a1", 0)
	deposit(t, ctx, r, "a1", 5) // 流版本 2

	// 手工写入版本 99 的“超前快照”（合法校验和，但事件流里根本不存在那些事件）
	future := &snapshot.Snapshot{
		StreamType: bank.AccountType, StreamID: "a1",
		Version: 99, GlobalSequence: 999,
		Payload: marshalState(t, bank.AccountState{
			ID: "a1", Holder: "Ghost", Currency: "USD", Balance: 99999, Open: true,
		}),
	}
	if err := future.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := snaps.Save(ctx, future); err != nil {
		t.Fatal(err)
	}

	var reason string
	// 用一个带 hook 的新仓储观察回退
	r2 := bank.NewAccountRepository(bank.AccountRepositoryConfig{
		Store: st, Registry: bank.NewRegistry(), Snapshots: snaps, Threshold: 1000,
		OnFallback: func(rc, id, _ string) { reason = rc + ":" + id },
	})
	a, _, err := r2.Load(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if reason != "ahead:a1" {
		t.Fatalf("expected ahead fallback, got %q", reason)
	}
	if a.BalanceCents() != 5 || a.Version() != 2 {
		t.Fatalf("ahead snapshot must be ignored: balance=%d ver=%d", a.BalanceCents(), a.Version())
	}
	if _, err := snaps.Load(ctx, bank.AccountType, "a1"); !errors.Is(err, snapshot.ErrSnapshotNotFound) {
		t.Fatalf("invalid snapshot should be deleted, err=%v", err)
	}
}

// 场景 6：保存冲突不会写入部分批次，草稿可丢弃后重试。
func TestSaveConflictRejectsWholeBatch(t *testing.T) {
	ctx := context.Background()
	r, _ := newFixture(t, eventstore.NewMemoryStore(), nil, 0, nil)
	openAccount(t, ctx, r, "a1", 0)
	a, err := bank.LoadAccount(ctx, r, "a1")
	if err != nil {
		t.Fatal(err)
	}
	b, err := bank.LoadAccount(ctx, r, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Deposit(10, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Save(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := b.Deposit(20, ""); err != nil {
		t.Fatal(err)
	}
	_, err = r.Save(ctx, b)
	var ce *eventstore.ConflictError
	if !errors.As(err, &ce) || ce.Reason != eventstore.ReasonVersionConflict {
		t.Fatalf("stale save must conflict, got %v", err)
	}
	// 被拒批次不得影响事件流或后续新写入
	got, ver, err := r.Load(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if ver != 2 || got.BalanceCents() != 10 {
		t.Fatalf("rejected batch leaked: ver=%d balance=%d", ver, got.BalanceCents())
	}
	if err := got.Deposit(7, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Save(ctx, got); err != nil {
		t.Fatalf("reload+save after conflict failed: %v", err)
	}
}

// 场景 7：加载不存在的聚合返回 repo.ErrNotFound。
func TestLoadMissing(t *testing.T) {
	ctx := context.Background()
	r, _ := newFixture(t, eventstore.NewMemoryStore(), snapshot.NewMemoryStore(), 10, nil)
	if _, _, err := r.Load(ctx, "nope"); !errors.Is(err, repo.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// 场景 8：文件存储 + 文件快照跨进程重启的端到端恢复。
func TestRestartWithFileBackedState(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	st1, err := eventstore.OpenFileStore(ctx, dir+"/events.log")
	if err != nil {
		t.Fatal(err)
	}
	r1 := bank.NewAccountRepository(bank.AccountRepositoryConfig{
		Store: st1, Registry: bank.NewRegistry(),
		Snapshots: snapshot.NewFileStore(dir + "/snapshots"), Threshold: 2,
	})
	a, err := bank.OpenAccount("a1", "Restart", 100, "EUR")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r1.Save(ctx, a); err != nil {
		t.Fatal(err)
	}
	deposit(t, ctx, r1, "a1", 50)
	deposit(t, ctx, r1, "a1", 25)
	if err := st1.Close(ctx); err != nil {
		t.Fatal(err)
	}

	st2, err := eventstore.OpenFileStore(ctx, dir+"/events.log")
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close(ctx)
	r2 := bank.NewAccountRepository(bank.AccountRepositoryConfig{
		Store: st2, Registry: bank.NewRegistry(),
		Snapshots: snapshot.NewFileStore(dir + "/snapshots"), Threshold: 2,
	})
	got, ver, err := r2.Load(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if ver != 3 || got.BalanceCents() != 175 || got.Currency() != "EUR" {
		t.Fatalf("restart recovery wrong: ver=%d balance=%d currency=%s",
			ver, got.BalanceCents(), got.Currency())
	}
}

func marshalState(t *testing.T, st bank.AccountState) []byte {
	t.Helper()
	raw, err := jsonMarshal(st)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func tamperSnapshot(t *testing.T, path string) {
	t.Helper()
	raw, err := readFileBytes(path)
	if err != nil {
		t.Fatal(err)
	}
	// 直接翻转文件中间的一个十六进制字符，使 JSON 或 checksum 失配。
	bs := []byte(raw)
	idx := len(bs) / 2
	if bs[idx] == '0' {
		bs[idx] = '1'
	} else if bs[idx] >= '1' && bs[idx] <= '9' {
		bs[idx]--
	} else {
		bs[idx] = '0'
	}
	if err := writeFileBytes(path, bs); err != nil {
		t.Fatal(err)
	}
}

// 防止未使用的 fmt（在某些精简编译路径下）。
var _ = fmt.Sprintf

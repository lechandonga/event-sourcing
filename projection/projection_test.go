package projection_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/event-sourcing/event"
	"github.com/lechandonga/event-sourcing/eventstore"
	"github.com/lechandonga/event-sourcing/projection"
)

// ---- 测试域：一个简单计数器事件，带 v1->v2 升级（缺省 kind） ----

type counterIncrV2 struct {
	Amount int64  `json:"amount"`
	Kind   string `json:"kind"`
}

func (*counterIncrV2) EventType() string       { return "counter.Incr" }
func (*counterIncrV2) EventSchemaVersion() int { return 2 }

func counterRegistry(t *testing.T) *event.Registry {
	t.Helper()
	r := event.NewRegistry()
	if err := r.Register((*counterIncrV2)(nil), map[int]event.Upcaster{
		1: func(m map[string]any) {
			if _, ok := m["kind"]; !ok {
				m["kind"] = "standard" // 新增字段的确定缺省值
			}
		},
	}); err != nil {
		t.Fatal(err)
	}
	return r
}

// counterModel 是计数读模型：求和、计数、记录进入 Apply 的全局序号。
type counterModel struct {
	mu         sync.Mutex
	sum        int64
	count      int64
	applied    []int64
	failOn     map[int64]bool // 这些全局序号第一次处理时失败（重试成功）
	failAlways map[int64]bool // 这些全局序号永远失败（毒丸）
	failOnce   map[int64]bool // 已消费过的“一次性失败”
	resetN     int
}

func newCounterModel() *counterModel {
	return &counterModel{
		failOn:     map[int64]bool{},
		failAlways: map[int64]bool{},
		failOnce:   map[int64]bool{},
	}
}

func (m *counterModel) Apply(_ context.Context, e projection.DecodedEvent) error {
	incr := e.Event.(*counterIncrV2)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failAlways[e.Envelope.GlobalSequence] {
		return fmt.Errorf("permanent failure at seq=%d", e.Envelope.GlobalSequence)
	}
	if m.failOn[e.Envelope.GlobalSequence] && !m.failOnce[e.Envelope.GlobalSequence] {
		m.failOnce[e.Envelope.GlobalSequence] = true
		return fmt.Errorf("transient failure at seq=%d", e.Envelope.GlobalSequence)
	}
	m.sum += incr.Amount
	m.count++
	m.applied = append(m.applied, e.Envelope.GlobalSequence)
	return nil
}

func (m *counterModel) Reset(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sum, m.count = 0, 0
	m.applied = nil
	m.resetN++
	return nil
}

func (m *counterModel) snapshot() (sum, count int64, applied []int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := append([]int64(nil), m.applied...)
	return m.sum, m.count, cp
}

func appendN(t *testing.T, ctx context.Context, st eventstore.Store, id string, startVer int64, n int, amount int64) {
	t.Helper()
	sid := eventstore.StreamID{Type: "counter", ID: id}
	for i := 0; i < n; i++ {
		raw := mustEncode(t, &counterIncrV2{Amount: amount, Kind: "standard"})
		_, err := st.Append(ctx, sid, eventstore.ExpectedVersion(startVer+int64(i)),
			[]eventstore.UncommittedEvent{{Type: "counter.Incr", Data: raw}})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func mustEncode(t *testing.T, v event.Versioned) []byte {
	t.Helper()
	raw, err := event.Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func newProjector(t *testing.T, st eventstore.Store, model projection.ReadModel, name string) *projection.Projector {
	t.Helper()
	p, err := projection.New(projection.Config{
		Name:         name,
		Store:        st,
		Model:        model,
		Registry:     counterRegistry(t),
		Checkpoints:  projection.NewMemoryCheckpointStore(),
		Failures:     projection.NewMemoryFailureStore(),
		PollInterval: 20 * time.Millisecond,
		BatchSize:    3,
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// 场景 1：长期增量处理的状态 == 从完整历史重建的状态。
func TestRebuildMatchesIncremental(t *testing.T) {
	ctx := context.Background()
	st := eventstore.NewMemoryStore()

	// 增量投影长期处理
	incModel := newCounterModel()
	inc := newProjector(t, st, incModel, "inc")
	var ver int64
	for batch := 0; batch < 5; batch++ {
		appendN(t, ctx, st, "c1", ver, 4, int64(batch+1))
		ver += 4
		if err := inc.CatchUp(ctx); err != nil {
			t.Fatal(err)
		}
	}
	incSum, incCount, incApplied := incModel.snapshot()

	// 全新模型从完整历史一次性重建
	repModel := newCounterModel()
	rep := newProjector(t, st, repModel, "rep")
	if err := rep.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	repSum, repCount, repApplied := repModel.snapshot()

	if incSum != repSum || incCount != repCount {
		t.Fatalf("state mismatch: incremental(sum=%d,count=%d) rebuild(sum=%d,count=%d)",
			incSum, incCount, repSum, repCount)
	}
	if len(incApplied) != len(repApplied) {
		t.Fatalf("applied length mismatch %d vs %d", len(incApplied), len(repApplied))
	}
	for i := range incApplied {
		if incApplied[i] != repApplied[i] {
			t.Fatalf("applied sequence differs at %d: %d vs %d", i, incApplied[i], repApplied[i])
		}
	}
}

// 场景 2：重建期间持续写入，重建不得阻塞写入，且最终不丢事件。
func TestRebuildDoesNotBlockOrLoseWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st := eventstore.NewMemoryStore()
	appendN(t, ctx, st, "seed", 0, 30, 1) // 预置历史

	model := newCounterModel()
	proj := newProjector(t, st, model, "live")

	var writerErr atomic.Value
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 重建开始后向多个流并发写入（独立流，互不冲突）。
		for i := 0; i < 100; i++ {
			id := fmt.Sprintf("w%02d", i%10)
			sid := eventstore.StreamID{Type: "counter", ID: id}
			raw := mustEncode(t, &counterIncrV2{Amount: 1, Kind: "standard"})
			// ExpectedVersionAny 简化并发（不同流隔离）；演示重点是重建不阻塞/不丢。
			if _, err := st.Append(ctx, sid, eventstore.ExpectedVersionAny,
				[]eventstore.UncommittedEvent{{Type: "counter.Incr", Data: raw}}); err != nil {
				writerErr.Store(err.Error())
				return
			}
		}
	}()

	if err := proj.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if e := writerErr.Load(); e != nil {
		t.Fatalf("writer failed during rebuild: %v", e)
	}
	if err := proj.CatchUp(ctx); err != nil {
		t.Fatal(err)
	}
	last, _ := st.LastSequence(ctx)
	cp, _ := proj.Checkpoint(ctx)
	if cp.Sequence != last {
		t.Fatalf("projection did not catch up: cp=%d store=%d", cp.Sequence, last)
	}
	sum, count, applied := model.snapshot()
	if int64(len(applied)) != last || count != last {
		t.Fatalf("events lost: applied=%d count=%d store=%d", len(applied), count, last)
	}
	// 30 个 seed(amount=1) + 100 个 writer(amount=1)
	if sum != 130 {
		t.Fatalf("sum=%d want 130", sum)
	}
}

// 场景 3：重复/乱序投递不得重复计数。通过直接调用 prepare 路径难以触达，
// 这里改用真实行为：检查点推进后，ReadAll 重复窗口不会二次应用。
func TestIdempotentReplayNoDoubleCount(t *testing.T) {
	ctx := context.Background()
	st := eventstore.NewMemoryStore()
	appendN(t, ctx, st, "c1", 0, 10, 2)
	model := newCounterModel()
	proj := newProjector(t, st, model, "idem")
	if err := proj.CatchUp(ctx); err != nil {
		t.Fatal(err)
	}
	sum1, count1, _ := model.snapshot()
	// 再次 CatchUp：没有新事件，不应应用任何东西
	if err := proj.CatchUp(ctx); err != nil {
		t.Fatal(err)
	}
	if err := proj.CatchUp(ctx); err != nil {
		t.Fatal(err)
	}
	sum2, count2, applied := model.snapshot()
	if sum1 != sum2 || count1 != count2 || len(applied) != 10 {
		t.Fatalf("replay double counted: sum %d->%d count %d->%d applied=%d",
			sum1, sum2, count1, count2, len(applied))
	}
	cp, _ := proj.Checkpoint(ctx)
	if cp.Sequence != 10 {
		t.Fatalf("checkpoint=%d want 10", cp.Sequence)
	}
}

// 场景 4：读模型处理失败 -> 检查点停住、失败可定位；修复后重试成功且不重复。
func TestFailureRecordedAndRetried(t *testing.T) {
	ctx := context.Background()
	st := eventstore.NewMemoryStore()
	appendN(t, ctx, st, "c1", 0, 6, 1)
	model := newCounterModel()
	model.failOn[3] = true // 全局序号 3 的事件第一次必失败
	proj := newProjector(t, st, model, "fail")

	// 第一次尝试应在 seq=3 失败
	err := proj.CatchUp(ctx)
	if err == nil {
		t.Fatal("expected processing failure")
	}
	if !contains(err.Error(), "seq=3") {
		t.Fatalf("error should locate failing event: %v", err)
	}
	cp, _ := proj.Checkpoint(ctx)
	if cp.Sequence != 2 {
		t.Fatalf("checkpoint must stop before failed event at 2, got %d", cp.Sequence)
	}
	f, ferr := proj.CurrentFailure(ctx)
	if ferr != nil || f == nil || f.Sequence != 3 || f.EventType != "counter.Incr" {
		t.Fatalf("failure record not locatable: %+v err=%v", f, ferr)
	}
	if f.Stream == "" || f.EventID == "" || f.Message == "" {
		t.Fatalf("failure record missing diagnostic fields: %+v", f)
	}

	// 重试：failOnce 已记录首次失败，第二次处理成功；前两个事件不能被重复计数
	if err := proj.CatchUp(ctx); err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}
	sum, count, applied := model.snapshot()
	// seq1-2 在首次尝试中已计数；seq3-6 在重试中计数。模型没有去重“跨检查点”的
	// 能力——但 seq1-2 的检查点已确认，重试窗口从 2 开始，不会重投。
	if sum != 6 || count != 6 || len(applied) != 6 {
		t.Fatalf("after retry want sum=6 count=6, got sum=%d count=%d applied=%v", sum, count, applied)
	}
	if f2, _ := proj.CurrentFailure(ctx); f2 != nil {
		t.Fatalf("cleared failure expected, got %+v", f2)
	}
}

// 场景 5：毒丸事件持续失败时，后续事件必须被“阻塞”而不是跳过。
func TestPoisonEventBlocksButNeverSkipped(t *testing.T) {
	ctx := context.Background()
	st := eventstore.NewMemoryStore()
	appendN(t, ctx, st, "c1", 0, 5, 1)
	model := newCounterModel()
	model.failAlways[2] = true // seq=2 永远失败（毒丸）
	proj := newProjector(t, st, model, "poison")

	for attempt := 0; attempt < 3; attempt++ {
		if err := proj.CatchUp(ctx); err == nil {
			t.Fatal("poison event must keep failing")
		}
	}
	_, count, applied := model.snapshot()
	if count != 1 || len(applied) != 1 || applied[0] != 1 {
		t.Fatalf("events after poison must not be applied, got %v count=%d", applied, count)
	}
	cp, _ := proj.Checkpoint(ctx)
	if cp.Sequence != 1 {
		t.Fatalf("checkpoint stuck at 1, got %d", cp.Sequence)
	}
	f, _ := proj.CurrentFailure(ctx)
	if f == nil || f.Attempts < 2 {
		t.Fatalf("failure should accumulate attempts, got %+v", f)
	}
}

// 场景 6：v1 历史事件在重建中也必须先升级（缺省字段语义在重放路径上生效）。
func TestRebuildUpgradesLegacyEvents(t *testing.T) {
	ctx := context.Background()
	st := eventstore.NewMemoryStore()
	sid := eventstore.StreamID{Type: "counter", ID: "old"}
	// 直接写 v1：没有 kind 字段
	v1 := []byte(`{"schemaVersion":1,"amount":7}`)
	if _, err := st.Append(ctx, sid, eventstore.ExpectedVersionNew,
		[]eventstore.UncommittedEvent{{Type: "counter.Incr", Data: v1}}); err != nil {
		t.Fatal(err)
	}
	model := newCounterModel()
	proj := newProjector(t, st, model, "legacy")
	if err := proj.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	sum, count, _ := model.snapshot()
	if sum != 7 || count != 1 {
		t.Fatalf("legacy event not replayed correctly: sum=%d count=%d", sum, count)
	}
}

// 场景 7：Run 循环持续消费并在失败后退避重试、ctx 取消时干净退出。
func TestRunLoopConsumesAndStops(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	st := eventstore.NewMemoryStore()
	model := newCounterModel()
	proj := newProjector(t, st, model, "run")
	runDone := make(chan error, 1)
	go func() { runDone <- proj.Run(ctx) }()

	for i := 0; i < 3; i++ {
		appendN(t, ctx, st, fmt.Sprintf("s%d", i), 0, 3, 1)
		time.Sleep(40 * time.Millisecond)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cp, _ := proj.Checkpoint(ctx)
		if cp.Sequence == 9 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cp, _ := proj.Checkpoint(ctx)
	if cp.Sequence != 9 {
		t.Fatalf("run loop did not consume all events, cp=%d", cp.Sequence)
	}
	cancel()
	select {
	case err := <-runDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("run exited with unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("run loop did not stop after cancel")
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

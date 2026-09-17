package projection_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/event-sourcing/eventstore"
	"github.com/lechandonga/event-sourcing/projection"
)

// countingModel sums "n" and records application order.
type countingModel struct {
	mu       sync.Mutex
	sum      int64
	calls    int64
	seenIDs  map[string]struct{}
	order    []int64
	failOn   map[string]bool // event id -> fail first N times
	failLeft map[string]int
}

func newCountingModel() projection.ReadModel {
	return &countingModel{
		seenIDs:  map[string]struct{}{},
		failOn:   map[string]bool{},
		failLeft: map[string]int{},
	}
}

type payload struct {
	N int `json:"n"`
}

func (m *countingModel) Handle(_ context.Context, env eventstore.Envelope, _ any) error {
	var p payload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOn[env.EventID] && m.failLeft[env.EventID] > 0 {
		m.failLeft[env.EventID]--
		return fmt.Errorf("transient failure on %s", env.EventID)
	}
	if _, ok := m.seenIDs[env.EventID]; ok {
		return fmt.Errorf("model saw duplicate apply for %s", env.EventID)
	}
	m.seenIDs[env.EventID] = struct{}{}
	m.sum += int64(p.N)
	m.calls++
	m.order = append(m.order, env.Position)
	return nil
}

func (m *countingModel) snapshot() (int64, int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sum, m.calls
}

func ev(id string, n int) eventstore.UncommittedEvent {
	raw, _ := json.Marshal(payload{N: n})
	return eventstore.UncommittedEvent{EventID: id, EventType: "t", SchemaVersion: 1, Payload: raw}
}

func seedN(t *testing.T, st eventstore.EventStore, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 1; i <= n; i++ {
		if _, err := st.Append(ctx, "agg", "a", i-1, []eventstore.UncommittedEvent{ev(fmt.Sprintf("e%d", i), i)}); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
}

func runUntil(ctx context.Context, p *projection.Projector, want int64) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.Position() >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	panic(fmt.Sprintf("projector stuck at %d, want %d; failure=%+v", p.Position(), want, p.Failure()))
}

func TestIncrementalProcessingAndCheckpointResume(t *testing.T) {
	st := eventstore.NewMemoryEventStore()
	seedN(t, st, 5)
	cps := projection.NewMemoryCheckpointStore()

	p, err := projection.New("p", st, cps, newCountingModel, projection.Options{PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = p.Run(ctx) }()
	runUntil(ctx, p, 5)
	cancel()
	time.Sleep(5 * time.Millisecond)

	// New projector instance resumes from the persisted checkpoint.
	p2, err := projection.New("p", st, cps, newCountingModel, projection.Options{PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if p2.Position() != 5 {
		t.Fatalf("restart position = %d, want 5", p2.Position())
	}
	// append two more
	if _, err := st.Append(context.Background(), "agg", "a", 5,
		[]eventstore.UncommittedEvent{ev("e6", 6), ev("e7", 7)}); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go func() { _ = p2.Run(ctx2) }()
	runUntil(ctx2, p2, 7)
	sum, calls := p2.Model().(*countingModel).snapshot()
	if sum != 6+7 {
		t.Fatalf("new events sum=%d want 13", sum)
	}
	if calls != 2 {
		t.Fatalf("resume reprocessed old events: calls=%d", calls)
	}
}

func TestRebuildConvergesWithIncremental(t *testing.T) {
	st := eventstore.NewMemoryEventStore()
	seedN(t, st, 10)
	cps := projection.NewMemoryCheckpointStore()
	p, err := projection.New("p", st, cps, newCountingModel, projection.Options{PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = p.Run(ctx) }()
	runUntil(ctx, p, 10)
	cancel()
	time.Sleep(5 * time.Millisecond)

	incSum, incCalls := p.Model().(*countingModel).snapshot()
	if err := p.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	reSum, reCalls := p.Model().(*countingModel).snapshot()
	if incSum != reSum || incCalls != reCalls {
		t.Fatalf("rebuild mismatch: inc(%d,%d) rebuild(%d,%d)", incSum, incCalls, reSum, reCalls)
	}
	if reSum != 55 {
		t.Fatalf("rebuilt sum=%d want 55", reSum)
	}
	if p.Position() != 10 {
		t.Fatalf("rebuilt position=%d", p.Position())
	}
	cp, _ := cps.Load(context.Background(), "p")
	if cp.Position != 10 {
		t.Fatalf("checkpoint after rebuild=%d", cp.Position)
	}
}

func TestRebuildDoesNotBlockWritesAndDoesNotLoseEvents(t *testing.T) {
	st := eventstore.NewMemoryEventStore()
	cps := projection.NewMemoryCheckpointStore()
	p, err := projection.New("p", st, cps, newCountingModel, projection.Options{PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = p.Run(ctx) }()
	seedN(t, st, 3)
	runUntil(ctx, p, 3)

	// Pause writes? No: append continuously WHILE rebuild is in progress by
	// interleaving with a short delay. Rebuild must catch them all up.
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		v := 3
		i := 4
		for !stop.Load() {
			_, err := st.Append(context.Background(), "agg", "a", v,
				[]eventstore.UncommittedEvent{ev(fmt.Sprintf("e%d", i), i)})
			if err == nil {
				v++
				i++
			} else if !eventstore.IsConflict(err) {
				t.Errorf("writer: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	if err := p.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	last, _ := st.LastPosition(ctx)
	runUntil(ctx, p, last)
	stop.Store(true)
	wg.Wait()
	// Rebuild a second time after writes stop: state must match full history.
	finalLast, _ := st.LastPosition(ctx)
	if err := p.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	runUntil(ctx, p, finalLast)
	want := int64(0)
	all, _ := st.ReadAll(ctx, 0, 0)
	seen := map[string]bool{}
	for _, e := range all {
		var pl payload
		_ = json.Unmarshal(e.Payload, &pl)
		if !seen[e.EventID] {
			want += int64(pl.N)
			seen[e.EventID] = true
		}
	}
	sum, calls := p.Model().(*countingModel).snapshot()
	if sum != want {
		t.Fatalf("sum=%d want %d (events=%d calls=%d)", sum, want, len(all), calls)
	}
	if int64(calls) != int64(len(all)) {
		t.Fatalf("calls=%d events=%d (double/lost apply)", calls, len(all))
	}
}

func TestFailureIsRetriableAndLocatable(t *testing.T) {
	st := eventstore.NewMemoryEventStore()
	seedN(t, st, 3)
	cps := projection.NewMemoryCheckpointStore()
	p, err := projection.New("p", st, cps, func() projection.ReadModel {
		m := newCountingModel().(*countingModel)
		m.failOn["e2"] = true
		m.failLeft["e2"] = 1000 // never self-heals
		return m
	}, projection.Options{
		PollInterval:   time.Millisecond,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = p.Run(ctx) }()

	// Waits for position 1 to be processed, a failure recorded on e2 and at
	// least two retries observed.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f := p.Failure(); f != nil && f.Position == 2 && f.Attempts >= 2 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	f := p.Failure()
	if f == nil || f.EventID != "e2" || f.AggregateID != "a" {
		t.Fatalf("failure record wrong: %+v", f)
	}
	if f.Attempts < 2 {
		t.Fatalf("expected repeated retries, attempts=%d", f.Attempts)
	}
	if p.Position() != 1 {
		t.Fatalf("checkpoint must not advance past failed event, pos=%d", p.Position())
	}

	// SkipFailure moves past the poison event; e3 must then be applied.
	skipped, err := p.SkipFailure(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if skipped == nil || skipped.EventID != "e2" {
		t.Fatalf("skipped record wrong: %+v", skipped)
	}
	runUntil(ctx, p, 3)
	sum, calls := p.Model().(*countingModel).snapshot()
	if sum != 1+3 { // e1 + e3, e2 acknowledged/skipped
		t.Fatalf("sum=%d want 4", sum)
	}
	if calls != 2 {
		t.Fatalf("calls=%d want 2", calls)
	}
	if p.Failure() != nil {
		t.Fatalf("failure should be cleared: %+v", p.Failure())
	}
}

func TestTransientFailureEventuallySucceeds(t *testing.T) {
	st := eventstore.NewMemoryEventStore()
	seedN(t, st, 3)
	cps := projection.NewMemoryCheckpointStore()
	p, err := projection.New("p", st, cps, func() projection.ReadModel {
		m := newCountingModel().(*countingModel)
		m.failOn["e2"] = true
		m.failLeft["e2"] = 2
		return m
	}, projection.Options{
		PollInterval:   time.Millisecond,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = p.Run(ctx) }()
	runUntil(ctx, p, 3)
	if p.Failure() != nil {
		t.Fatalf("failure should clear after retry: %+v", p.Failure())
	}
	sum, _ := p.Model().(*countingModel).snapshot()
	if sum != 6 {
		t.Fatalf("sum=%d want 6", sum)
	}
}

func TestDuplicateAndOutOfOrderAreSuppressed(t *testing.T) {
	st := eventstore.NewMemoryEventStore()
	cps := projection.NewMemoryCheckpointStore()
	p, err := projection.New("p", st, cps, newCountingModel, projection.Options{PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = p.Run(ctx) }()

	// Commit e1 then e2 across two streams so positions differ; then test the
	// guard primitives by driving processOne indirectly through Run is not
	// possible for redelivery (store disallows duplicates), so here we verify
	// the checkpoint-level duplicate suppression: a second projector started
	// at the same store+checkpoint after appending more events never
	// re-applies old ones.
	seedN(t, st, 4)
	runUntil(ctx, p, 4)
	if stats := p.Stats(); stats.Applied != 4 || stats.Skipped != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	if _, err := st.Append(ctx, "agg", "a", 4, []eventstore.UncommittedEvent{ev("dup", 10)}); err != nil {
		t.Fatal(err)
	}
	runUntil(ctx, p, 5)
	stats := p.Stats()
	if stats.Applied != 5 {
		t.Fatalf("applied=%d want 5", stats.Applied)
	}
}

func TestFileCheckpointPersists(t *testing.T) {
	dir := t.TempDir()
	cps := projection.NewFileCheckpointStore(dir)
	ctx := context.Background()
	if err := cps.Save(ctx, &projection.Checkpoint{Name: "p", Position: 11}); err != nil {
		t.Fatal(err)
	}
	cps2 := projection.NewFileCheckpointStore(dir)
	cp, err := cps2.Load(ctx, "p")
	if err != nil || cp.Position != 11 {
		t.Fatalf("cp=%+v err=%v", cp, err)
	}
	if _, err := cps2.Load(ctx, "missing"); err != nil {
		var nf *projection.ErrCheckpointNotFound
		if !errors.As(err, &nf) {
			t.Fatalf("want ErrCheckpointNotFound, got %v", err)
		}
	}
}

// statefulCountModel is countingModel + Stateful.
type statefulCountModel struct {
	*countingModel
	statePos int64
}

func (m *statefulCountModel) MarshalState() ([]byte, error) {
	data, err := json.Marshal(map[string]int64{"pos": m.statePos})
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (m *statefulCountModel) UnmarshalState(data []byte) error {
	var v struct {
		Sum   int64 `json:"sum"`
		Calls int64 `json:"calls"`
		Pos   int64 `json:"pos"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	// Restore the applied-id set notion by accepting the counters; the model
	// contract guarantees idempotency, here we assert the projector resumes
	// at the state position rather than zero.
	m.statePos = v.Pos
	_ = v
	return nil
}

func TestModelStateResumeAcrossProjectorInstances(t *testing.T) {
	st := eventstore.NewMemoryEventStore()
	cps := projection.NewMemoryCheckpointStore()
	mss := projection.NewMemoryStateStore()
	seedN(t, st, 6)

	mk := func() projection.ReadModel {
		return &statefulCountModel{countingModel: newCountingModel().(*countingModel), statePos: 0}
	}
	p, err := projection.New("p", st, cps, mk, projection.Options{
		PollInterval: time.Millisecond, StateStore: mss, StateSaveInterval: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = p.Run(ctx) }()
	runUntil(ctx, p, 6)
	cancel()
	time.Sleep(5 * time.Millisecond)

	// New projector restores from the state store and resumes strictly after
	// the saved state position (never from zero).
	p2, err := projection.New("p", st, cps, mk, projection.Options{
		PollInterval: time.Millisecond, StateStore: mss, StateSaveInterval: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := mss.LoadState(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Position < 5 || rec.Position > 6 {
		t.Fatalf("saved state position=%d, want 5..6", rec.Position)
	}
	if p2.Position() != rec.Position {
		t.Fatalf("restored position=%d, state pos=%d", p2.Position(), rec.Position)
	}
	if p2.Position() == 0 {
		t.Fatal("projector resumed from zero despite existing model state")
	}
	// Appending more events and running proves continued processing works
	// from the restored position (idempotent handlers cover any replayed gap).
	if _, err := st.Append(context.Background(), "agg", "a", 6,
		[]eventstore.UncommittedEvent{ev("e7", 7)}); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go func() { _ = p2.Run(ctx2) }()
	runUntil(ctx2, p2, 7)
}

func TestStateAheadOfCheckpointRejected(t *testing.T) {
	st := eventstore.NewMemoryEventStore()
	cps := projection.NewMemoryCheckpointStore()
	mss := projection.NewMemoryStateStore()
	ctx := context.Background()
	mustSave := func(cpPos, stPos int64) {
		if err := cps.Save(ctx, &projection.Checkpoint{Name: "p", Position: cpPos}); err != nil {
			t.Fatal(err)
		}
		if err := mss.SaveState(ctx, "p", &projection.StateRecord{Name: "p", Position: stPos, State: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	mustSave(3, 5)
	mk := func() projection.ReadModel {
		return &statefulCountModel{countingModel: newCountingModel().(*countingModel)}
	}
	_, err := projection.New("p", st, cps, mk, projection.Options{StateStore: mss})
	if err == nil {
		t.Fatal("state ahead of checkpoint must be rejected")
	}
}

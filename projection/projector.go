// Package projection implements read-model synchronization on top of the
// local event store:
//
//   - incremental processing from a persisted checkpoint;
//   - full Rebuild from position 0 that never blocks event writes and ends
//     with an atomic swap onto the rebuilt state;
//   - idempotency guards against duplicate, out-of-order and late-arriving
//     events;
//   - retriable failure records that pinpoint the offending event;
//   - explicit SkipFailure escape hatch for poison messages.
package projection

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lechandonga/event-sourcing/event"
	"github.com/lechandonga/event-sourcing/eventstore"
)

// ModelFactory creates a fresh, empty read model. Used for initial state and
// for every Rebuild (the rebuilt model is constructed independently and only
// swapped in once fully caught up).
type ModelFactory func() ReadModel

// Options configures a Projector.
type Options struct {
	// BatchSize bounds one ReadAll round-trip. Default 500.
	BatchSize int
	// PollInterval is how long an idle projector waits before looking for
	// new events. Default 50ms.
	PollInterval time.Duration
	// InitialBackoff / MaxBackoff bound the exponential backoff applied while
	// an event keeps failing. Defaults 10ms / 1s.
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	// Registry optionally decodes/upcasts events before Handle.
	Registry *event.Registry
	// DedupWindow is how many already-processed event ids the projector
	// remembers to suppress in-memory redelivery (late/duplicate). Default 4096.
	DedupWindow int
	// OnSkip is invoked when an event is suppressed as duplicate/regressed.
	OnSkip func(env eventstore.Envelope, reason SkipReason)
	// StateStore optionally persists the read model state itself, so a
	// restarted projector restores both model and position without a rebuild.
	// The model produced by ModelFactory must implement Stateful.
	StateStore StateStore
	// StateSaveInterval persists model state every N committed positions
	// (minimum 1: the state at the final committed position is always written
	// on shutdown-free progress when the counter crosses the interval, and
	// always after Rebuild). Default 20 when a StateStore is configured.
	StateSaveInterval int64
}

func (o *Options) withDefaults() {
	if o.BatchSize <= 0 {
		o.BatchSize = eventstore.DefaultReadBatchSize
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 50 * time.Millisecond
	}
	if o.InitialBackoff <= 0 {
		o.InitialBackoff = 10 * time.Millisecond
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = time.Second
	}
	if o.DedupWindow <= 0 {
		o.DedupWindow = 4096
	}
	if o.StateSaveInterval == 0 && o.StateStore != nil {
		o.StateSaveInterval = 20
	}
}

// SkipReason says why an event was not handed to the model.
type SkipReason string

const (
	// SkipAlreadyProcessed: the event's position is <= the committed
	// checkpoint (classic redelivery after a crash/rebuffer).
	SkipAlreadyProcessed SkipReason = "already_processed"
	// SkipDuplicateID: another event with the same EventID was applied.
	SkipDuplicateID SkipReason = "duplicate_id"
	// SkipRegressedVersion: within one aggregate stream the event's version is
	// not greater than the last applied version (out-of-order / late).
	SkipRegressedVersion SkipReason = "regressed_stream_version"
)

// Failure is the locatable error record for the event currently blocking the
// projection. The checkpoint does not advance past this event until Handle
// succeeds (or SkipFailure is called), so the record always pinpoints the
// exact stream position requiring intervention.
type Failure struct {
	Position      int64
	EventID       string
	EventType     string
	AggregateType string
	AggregateID   string
	Attempts      int
	FirstAt       time.Time
	LastAt        time.Time
	Err           string
}

type streamKey struct {
	aggregateType string
	aggregateID   string
}

// Stats are processing counters.
type Stats struct {
	Applied         int64
	Skipped         int64
	SkippedByReason map[SkipReason]int64
}

// Projector drives one read model from one event store using one checkpoint.
type Projector struct {
	name    string
	store   eventstore.EventStore
	cps     CheckpointStore
	factory ModelFactory
	opts    Options

	stateMu   sync.RWMutex
	model     ReadModel
	pos       int64
	streamVer map[streamKey]int
	idOrder   []string
	idSet     map[string]struct{}

	failMu  sync.Mutex
	failure *Failure
	// waitCh is closed whenever the blocking failure is cleared (retry
	// succeeded or SkipFailure); Run waits on it between attempts.
	waitCh chan struct{}

	paused atomic.Bool
	// idle is closed while no batch is being processed; recreated on resume.
	idleMu sync.Mutex
	idle   chan struct{}

	metricsMu      sync.Mutex
	appliedCount   int64
	skippedCount   int64
	skippedReasons map[SkipReason]int64

	stateStore   StateStore
	lastStatePos int64
}

// New loads (or initializes) projector state. The model is queryable at once;
// processing starts when Run is invoked.
func New(name string, store eventstore.EventStore, cps CheckpointStore, factory ModelFactory, opts Options) (*Projector, error) {
	opts.withDefaults()
	p := &Projector{
		name:           name,
		store:          store,
		cps:            cps,
		factory:        factory,
		opts:           opts,
		model:          factory(),
		streamVer:      make(map[streamKey]int),
		idSet:          make(map[string]struct{}),
		waitCh:         make(chan struct{}),
		idle:           make(chan struct{}),
		skippedReasons: make(map[SkipReason]int64),
	}
	close(p.idle)
	cp, err := cps.Load(context.Background(), name)
	if err != nil {
		var nf *ErrCheckpointNotFound
		if !errors.As(err, &nf) {
			return nil, fmt.Errorf("projection: load checkpoint %q: %w", name, err)
		}
	} else {
		p.pos = cp.Position
	}

	// Restore model state when a state store is configured. The state may
	// lag behind the checkpoint (write state failed after checkpoint): we
	// simply replay from the state position. A state ahead of the checkpoint
	// is impossible with the write ordering below; refuse it loudly.
	if opts.StateStore != nil {
		rec, err := opts.StateStore.LoadState(context.Background(), name)
		if err == nil {
			sm, ok := p.model.(Stateful)
			if !ok {
				return nil, fmt.Errorf("projection: %q has StateStore but model does not implement Stateful", name)
			}
			if rec.Position > p.pos {
				return nil, fmt.Errorf("projection: model state position %d ahead of checkpoint %d", rec.Position, p.pos)
			}
			if err := sm.UnmarshalState(rec.State); err != nil {
				return nil, fmt.Errorf("projection: restore model state: %w", err)
			}
			p.pos = rec.Position
			p.lastStatePos = rec.Position
		} else {
			var nf *ErrStateNotFound
			if !errors.As(err, &nf) {
				return nil, fmt.Errorf("projection: load model state %q: %w", name, err)
			}
		}
		p.stateStore = opts.StateStore
	}
	return p, nil
}

// Model returns the current read model. After Rebuild the concrete value may
// change; re-fetch Model rather than retaining an old reference.
func (p *Projector) Model() ReadModel {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return p.model
}

// Position returns the last durably processed global position.
func (p *Projector) Position() int64 {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return p.pos
}

// Stats returns counters since projector creation.
func (p *Projector) Stats() Stats {
	p.metricsMu.Lock()
	defer p.metricsMu.Unlock()
	by := make(map[SkipReason]int64, len(p.skippedReasons))
	for k, v := range p.skippedReasons {
		by[k] = v
	}
	return Stats{Applied: p.appliedCount, Skipped: p.skippedCount, SkippedByReason: by}
}

// Failure returns a copy of the current blocking failure record, if any.
func (p *Projector) Failure() *Failure {
	p.failMu.Lock()
	defer p.failMu.Unlock()
	if p.failure == nil {
		return nil
	}
	cp := *p.failure
	return &cp
}

func (p *Projector) rememberID(id string) {
	if id == "" {
		return
	}
	if _, ok := p.idSet[id]; ok {
		return
	}
	p.idSet[id] = struct{}{}
	p.idOrder = append(p.idOrder, id)
	if len(p.idOrder) > p.opts.DedupWindow {
		drop := p.idOrder[0]
		p.idOrder = p.idOrder[1:]
		delete(p.idSet, drop)
	}
}

func (p *Projector) rememberVersion(t, id string, version int) {
	if version <= 0 {
		return
	}
	k := streamKey{t, id}
	if version > p.streamVer[k] {
		p.streamVer[k] = version
	}
}

func (p *Projector) noteSkip(reason SkipReason) {
	p.metricsMu.Lock()
	p.skippedCount++
	p.skippedReasons[reason]++
	p.metricsMu.Unlock()
}

func (p *Projector) noteApplied() {
	p.metricsMu.Lock()
	p.appliedCount++
	p.metricsMu.Unlock()
}

// SkipFailure acknowledges the blocking event after external reconciliation
// (or an explicit decision to drop it), commits the checkpoint past it and
// wakes Run immediately. The returned record documents the skipped event.
func (p *Projector) SkipFailure(ctx context.Context) (*Failure, error) {
	p.failMu.Lock()
	f := p.failure
	if f == nil {
		p.failMu.Unlock()
		return nil, nil
	}
	cpFail := *f
	ch := p.waitCh
	p.failure = nil
	p.waitCh = make(chan struct{})
	p.failMu.Unlock()

	p.stateMu.Lock()
	if cpFail.Position > p.pos {
		p.pos = cpFail.Position
		p.rememberID(cpFail.EventID)
		if err := p.commitCheckpointLocked(ctx); err != nil {
			// Checkpoint could not be persisted. Keep position at the last
			// committed boundary and reinstall the failure so the event
			// remains locatable and retriable.
			p.pos = cpFail.Position - 1
			p.stateMu.Unlock()
			p.failMu.Lock()
			p.failure = &cpFail
			p.failMu.Unlock()
			return nil, err
		}
	}
	p.stateMu.Unlock()
	close(ch)
	return &cpFail, nil
}

// commitCheckpointLocked persists p.pos and, when configured, periodically
// persists the model state at the same position. Caller holds stateMu.
//
// Ordering: model state is written FIRST with its embedded position, then the
// checkpoint. A crash between the two is safe on restart: the state simply
// lags the checkpoint and the gap is replayed (handlers are idempotent).
func (p *Projector) commitCheckpointLocked(ctx context.Context) error {
	if p.stateStore != nil {
		interval := p.opts.StateSaveInterval
		if interval < 1 {
			interval = 1
		}
		if p.pos-p.lastStatePos >= interval || p.lastStatePos == 0 {
			if sm, ok := p.model.(Stateful); ok {
				data, err := sm.MarshalState()
				if err != nil {
					return fmt.Errorf("projection: marshal model state: %w", err)
				}
				rec := &StateRecord{Name: p.name, Position: p.pos, State: data, UpdatedAt: time.Now().UTC()}
				if err := p.stateStore.SaveState(ctx, p.name, rec); err != nil {
					return fmt.Errorf("projection: save model state: %w", err)
				}
				p.lastStatePos = p.pos
			}
		}
	}
	cp := &Checkpoint{Name: p.name, Position: p.pos, UpdatedAt: time.Now().UTC()}
	if err := p.cps.Save(ctx, cp); err != nil {
		return fmt.Errorf("projection: save checkpoint %q: %w", p.name, err)
	}
	return nil
}

// processOne validates ordering/idempotency, then applies one event and
// advances the checkpoint. It must be called while the caller holds
// p.stateMu exclusively.
func (p *Projector) processOne(ctx context.Context, env eventstore.Envelope) error {
	if env.Position <= p.pos {
		p.noteSkip(SkipAlreadyProcessed)
		if p.opts.OnSkip != nil {
			p.opts.OnSkip(env, SkipAlreadyProcessed)
		}
		return nil
	}
	if env.EventID != "" {
		if _, dup := p.idSet[env.EventID]; dup {
			p.noteSkip(SkipDuplicateID)
			if p.opts.OnSkip != nil {
				p.opts.OnSkip(env, SkipDuplicateID)
			}
			// Still bookkeep position without invoking the handler.
			p.pos = env.Position
			p.rememberVersion(env.AggregateType, env.AggregateID, env.Version)
			return p.commitCheckpointLocked(ctx)
		}
	}
	k := streamKey{env.AggregateType, env.AggregateID}
	if env.Version <= p.streamVer[k] {
		p.noteSkip(SkipRegressedVersion)
		if p.opts.OnSkip != nil {
			p.opts.OnSkip(env, SkipRegressedVersion)
		}
		// Out-of-order/late event from a stream already ahead: it can never
		// be applied again. Advance position, keep stream watermark.
		p.pos = env.Position
		return p.commitCheckpointLocked(ctx)
	}

	var decoded any
	if p.opts.Registry != nil {
		v, err := p.opts.Registry.Decode(env.EventType, env.SchemaVersion, env.Payload)
		if err != nil {
			return err
		}
		decoded = v
	}
	if err := p.model.Handle(ctx, env, decoded); err != nil {
		return err
	}
	p.pos = env.Position
	p.rememberID(env.EventID)
	p.rememberVersion(env.AggregateType, env.AggregateID, env.Version)
	p.noteApplied()
	return p.commitCheckpointLocked(ctx)
}

// Run blocks, incrementally processing events from the checkpoint until ctx
// is canceled. A failure on one event pauses processing AT that event: the
// same event is retried with exponential backoff (its identity stays
// available via Failure), or SkipFailure advances past it.
func (p *Projector) Run(ctx context.Context) error {
	backoff := p.opts.InitialBackoff
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if p.paused.Load() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(p.opts.PollInterval):
			}
			continue
		}

		// If a failure is recorded, wait for backoff timeout OR external
		// clearance OR context cancellation before retrying.
		if f := p.Failure(); f != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-p.failWaitCh():
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > p.opts.MaxBackoff {
				backoff = p.opts.MaxBackoff
			}
		}

		p.markBusy()
		if p.paused.Load() {
			// Pause landed between the loop check and busy transition.
			p.markIdle()
			continue
		}
		events, err := p.store.ReadAll(ctx, p.snapshotPosition(), p.opts.BatchSize)
		if err != nil {
			p.markIdle()
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			p.recordFailure(eventstore.Envelope{}, err, false)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > p.opts.MaxBackoff {
				backoff = p.opts.MaxBackoff
			}
			continue
		}

		blocked := false
		ctxCanceled := false
		if len(events) > 0 {
			p.stateMu.Lock()
			var failedEnv eventstore.Envelope
			madeProgress := false
		loop:
			for _, env := range events {
				if err := ctx.Err(); err != nil {
					ctxCanceled = true
					break loop
				}
				if err := p.processOne(ctx, env); err != nil {
					failedEnv = env
					p.recordFailure(failedEnv, err, true)
					blocked = true
					break loop
				}
				madeProgress = true
			}
			// A successful retry consumed the previously blocking event:
			// drop its Failure record. A fresh failure above reinstalls one.
			if madeProgress && !blocked {
				p.clearFailure()
			}
			p.stateMu.Unlock()
			if ctxCanceled {
				p.markIdle()
				return ctx.Err()
			}
		}
		p.markIdle()

		if blocked {
			continue
		}
		backoff = p.opts.InitialBackoff
		if len(events) < p.opts.BatchSize {
			// Caught up; poll for new events.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(p.opts.PollInterval):
			}
		}
	}
}

func (p *Projector) snapshotPosition() int64 {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return p.pos
}

func (p *Projector) failWaitCh() chan struct{} {
	p.failMu.Lock()
	defer p.failMu.Unlock()
	return p.waitCh
}

// clearFailure removes any blocking failure record and installs a fresh wait
// channel. Called after a successful retry batch.
func (p *Projector) clearFailure() {
	p.failMu.Lock()
	defer p.failMu.Unlock()
	if p.failure == nil {
		return
	}
	p.failure = nil
	select {
	case <-p.waitCh:
		p.waitCh = make(chan struct{})
	default:
	}
}

func (p *Projector) recordFailure(env eventstore.Envelope, cause error, countAttempt bool) {
	p.failMu.Lock()
	defer p.failMu.Unlock()
	now := time.Now().UTC()
	if p.failure != nil && p.failure.Position == env.Position && env.Position != 0 {
		p.failure.Attempts++
		p.failure.LastAt = now
		p.failure.Err = cause.Error()
		return
	}
	// A new blocking position: install a fresh wait channel (the previous
	// one may already be closed by SkipFailure).
	select {
	case <-p.waitCh:
		p.waitCh = make(chan struct{})
	default:
	}
	attempts := 1
	if !countAttempt {
		attempts = 0
	}
	p.failure = &Failure{
		Position:      env.Position,
		EventID:       env.EventID,
		EventType:     env.EventType,
		AggregateType: env.AggregateType,
		AggregateID:   env.AggregateID,
		Attempts:      attempts,
		FirstAt:       now,
		LastAt:        now,
		Err:           cause.Error(),
	}
}

func (p *Projector) markBusy() {
	p.idleMu.Lock()
	defer p.idleMu.Unlock()
	if p.paused.Load() {
		return
	}
	select {
	case <-p.idle:
		// currently idle -> transition to busy
		p.idle = make(chan struct{})
	default:
	}
}

func (p *Projector) markIdle() {
	p.idleMu.Lock()
	defer p.idleMu.Unlock()
	select {
	case <-p.idle:
		// already closed/idle
	default:
		close(p.idle)
	}
}

func (p *Projector) waitIdle(ctx context.Context) error {
	for {
		p.idleMu.Lock()
		ch := p.idle
		paused := p.paused.Load()
		p.idleMu.Unlock()
		select {
		case <-ch:
			if paused {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

// PauseRequest is returned by Pause and resolved by Resume.
type PauseRequest struct{ p *Projector }

// Resume unblocks processing.
func (r PauseRequest) Resume() { r.p.Resume() }

// Pause stops the incremental loop from fetching/applying new batches and
// returns once any in-flight batch has finished. Event APPENDS are never
// blocked: the pause affects only this projector.
func (p *Projector) Pause() PauseRequest {
	p.paused.Store(true)
	for {
		p.idleMu.Lock()
		ch := p.idle
		p.idleMu.Unlock()
		<-ch
		// One more iteration guarantees we did not observe the pre-busy
		// channel racing with markBusy.
		p.idleMu.Lock()
		ch2 := p.idle
		p.idleMu.Unlock()
		if ch2 == ch {
			break
		}
	}
	return PauseRequest{p: p}
}

// Resume re-enables processing.
func (p *Projector) Resume() {
	p.paused.Store(false)
}

// Rebuild rebuilds the read model from the COMPLETE event history and swaps
// the rebuilt model in atomically once it has caught up with all events
// committed up to the catch-up moment. Event writes to the store proceed
// unblocked the entire time.
//
// Guarantees:
//   - the store is read from position 0 through a fresh model, so every
//     committed event is seen exactly once in order;
//   - a final lock-held catch-up drains events committed during the bulk
//     replay, so no event is lost across the swap;
//   - the checkpoint is reset to 0 during rebuild and advanced under the same
//     lock while catching up; only then is the new model published;
//   - repeated rebuilds converge with incremental processing because both
//     apply the same deterministic upcast + Handle pipeline.
func (p *Projector) Rebuild(ctx context.Context) error {
	pauseReq := p.Pause()
	defer pauseReq.Resume()

	model := p.factory()
	session := &rebuildSession{
		projector: p,
		model:     model,
		pos:       0,
		streamVer: make(map[streamKey]int),
		idSet:     make(map[string]struct{}),
	}

	// Bulk replay WITHOUT the projector lock: appends and queries of the old
	// model are unaffected. Read in ascending global-position batches.
	from := int64(0)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, err := p.store.ReadAll(ctx, from, p.opts.BatchSize)
		if err != nil {
			return fmt.Errorf("projection: rebuild read: %w", err)
		}
		if len(batch) == 0 {
			break
		}
		for i := range batch {
			if err := session.apply(ctx, batch[i]); err != nil {
				return fmt.Errorf("projection: rebuild at position %d: %w", batch[i].Position, err)
			}
			from = batch[i].Position
		}
		if len(batch) < p.opts.BatchSize {
			break
		}
	}

	// Atomic catch-up + swap under the exclusive state lock. Because Run is
	// paused, no one else mutates projector state; new store appends are not
	// blocked, they simply wait here in the log until drained below.
	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	catchFrom := session.pos
	for {
		batch, err := p.store.ReadAll(ctx, catchFrom, p.opts.BatchSize)
		if err != nil {
			return fmt.Errorf("projection: rebuild catch-up read: %w", err)
		}
		if len(batch) == 0 {
			break
		}
		for i := range batch {
			if err := session.apply(ctx, batch[i]); err != nil {
				return fmt.Errorf("projection: rebuild catch-up at position %d: %w", batch[i].Position, err)
			}
			catchFrom = batch[i].Position
		}
		if len(batch) < p.opts.BatchSize {
			break
		}
	}

	p.model = session.model
	p.pos = session.pos
	p.streamVer = session.streamVer
	p.idOrder = session.idOrder
	p.idSet = session.idSet
	// The state store now refers to the previous model: force a fresh model
	// state write at the rebuilt position.
	p.lastStatePos = 0
	if err := p.commitCheckpointLocked(ctx); err != nil {
		return fmt.Errorf("projection: rebuild checkpoint: %w", err)
	}
	return nil
}

// rebuildSession is the private state of one rebuild; it deliberately does
// not touch the live projector state until the final swap.
type rebuildSession struct {
	projector *Projector
	model     ReadModel
	pos       int64
	streamVer map[streamKey]int
	idSet     map[string]struct{}
	idOrder   []string
}

func (s *rebuildSession) apply(ctx context.Context, env eventstore.Envelope) error {
	// In rebuild we read strictly ascending positions from the store; keep
	// the same idempotency guards so a duplicated event in history (which
	// the store forbids, but defend in depth) can never double count.
	if env.Position <= s.pos {
		s.projector.noteSkip(SkipAlreadyProcessed)
		return nil
	}
	if env.EventID != "" {
		if _, dup := s.idSet[env.EventID]; dup {
			s.projector.noteSkip(SkipDuplicateID)
			s.pos = env.Position
			return nil
		}
	}
	k := streamKey{env.AggregateType, env.AggregateID}
	if env.Version <= s.streamVer[k] {
		s.projector.noteSkip(SkipRegressedVersion)
		s.pos = env.Position
		return nil
	}
	var decoded any
	if s.projector.opts.Registry != nil {
		v, err := s.projector.opts.Registry.Decode(env.EventType, env.SchemaVersion, env.Payload)
		if err != nil {
			return err
		}
		decoded = v
	}
	if err := s.model.Handle(ctx, env, decoded); err != nil {
		return err
	}
	s.pos = env.Position
	s.streamVer[k] = env.Version
	if env.EventID != "" {
		s.idSet[env.EventID] = struct{}{}
		s.idOrder = append(s.idOrder, env.EventID)
		if len(s.idOrder) > s.projector.opts.DedupWindow {
			drop := s.idOrder[0]
			s.idOrder = s.idOrder[1:]
			delete(s.idSet, drop)
		}
	}
	s.projector.noteApplied()
	return nil
}

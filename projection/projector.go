package projection

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/lechandonga/event-sourcing/event"
	"github.com/lechandonga/event-sourcing/eventstore"
)

// DecodedEvent 是完成升级与解码、可供读模型消费的事件。
type DecodedEvent struct {
	// Event 是升级到当前 schema 后的 Go 事件结构。
	Event any
	// Envelope 是已提交的原始信封（原始 Data 永不被改写）。
	Envelope eventstore.RecordedEvent
	// OriginalVersion 是该事件存储时的 schema 版本；与当前版本相同时说明没有升级。
	OriginalVersion int
}

// ReadModel 是投影读模型。
//
// Apply 必须是确定性的纯状态转移：相同事件序列必须得到相同状态
// （这是“重建结果 == 长期增量结果”的根本保证）。
type ReadModel interface {
	Apply(ctx context.Context, e DecodedEvent) error
}

// Batcher 是读模型可选实现的接口：批量 Apply 可减少锁竞争。
// 投影仪会先对批次做去重、排序与升级，再整体交给读模型。
type Batcher interface {
	ApplyBatch(ctx context.Context, events []DecodedEvent) error
}

// Config 是 Projector 的配置。
type Config struct {
	// Name 是投影名（检查点/失败记录的键），必须稳定且唯一。
	Name string
	// Store 是事件存储。
	Store eventstore.Store
	// Model 是读模型。
	Model ReadModel
	// Registry 用于把历史事件升级并解码为当前结构。
	Registry *event.Registry
	// Checkpoints 持久化检查点。
	Checkpoints CheckpointStore
	// Failures 持久化阻塞失败；为 nil 时失败仅返回错误、不留盘。
	Failures FailureStore
	// PollInterval 是无订阅事件时的兜底轮询间隔；默认 500ms。
	PollInterval time.Duration
	// BatchSize 是每次拉取的最大事件数；默认 500。
	BatchSize int64
	// Logger 默认为 slog.Default()。
	Logger *slog.Logger
}

// Projector 驱动单个读模型的增量处理与重建。
//
// 一个 Projector 串行地推进自己的检查点，因此同一份读模型/检查点
// 在任一时刻只能有一个 Projector 运行。重建与增量推进共用本结构，
// 二者绝不会并发操作同一个读模型。
type Projector struct {
	cfg       Config
	log       *slog.Logger
	poll      time.Duration
	batchSize int64

	runMu   sync.Mutex
	running bool
	sub     eventstore.Subscription
}

// New 构造投影仪并校验配置。
func New(cfg Config) (*Projector, error) {
	if cfg.Name == "" {
		return nil, errors.New("projection: Name is required")
	}
	if cfg.Store == nil || cfg.Model == nil || cfg.Registry == nil || cfg.Checkpoints == nil {
		return nil, errors.New("projection: Store, Model, Registry and Checkpoints are required")
	}
	poll := cfg.PollInterval
	if poll <= 0 {
		poll = 500 * time.Millisecond
	}
	batch := cfg.BatchSize
	if batch <= 0 {
		batch = 500
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Projector{cfg: cfg, log: log, poll: poll, batchSize: batch}, nil
}

// Checkpoint 返回当前持久化检查点。
func (p *Projector) Checkpoint(ctx context.Context) (Checkpoint, error) {
	return p.cfg.Checkpoints.Load(ctx, p.cfg.Name)
}

// CurrentFailure 返回当前阻塞失败；没有则为 nil。
func (p *Projector) CurrentFailure(ctx context.Context) (*Failure, error) {
	if p.cfg.Failures == nil {
		return nil, nil
	}
	return p.cfg.Failures.Load(ctx, p.cfg.Name)
}

// ProcessOnce 执行一轮“拉取 -> 升级 -> 去重排序 -> 应用 -> 推进检查点”。
//
// 返回 (processed, err)：processed 为本轮成功处理的事件数。
// 读模型对某事件返回错误时：
//   - 检查点停在该事件之前，事件不会被跳过；
//   - 失败信息（含全局序号/流/事件 ID/错误消息）写入 FailureStore，可定位、可重试；
//   - 返回错误，下一次 ProcessOnce/Run 会从同一事件重新尝试。
func (p *Projector) ProcessOnce(ctx context.Context) (int, error) {
	cp, err := p.cfg.Checkpoints.Load(ctx, p.cfg.Name)
	if err != nil {
		return 0, err
	}
	last, err := p.cfg.Store.LastSequence(ctx)
	if err != nil {
		return 0, err
	}
	if cp.Sequence >= last {
		return 0, nil
	}

	upTo := last
	if upTo-cp.Sequence > p.batchSize {
		upTo = cp.Sequence + p.batchSize
	}
	raw, err := p.cfg.Store.ReadAll(ctx, cp.Sequence, upTo)
	if err != nil {
		return 0, err
	}
	decoded, err := p.prepare(raw, cp.Sequence)
	if err != nil {
		return 0, err
	}
	if len(decoded) == 0 {
		return 0, nil
	}
	// 逐事件应用并在每个成功事件后立即前移检查点：
	// 这保证失败时检查点精确停在“毒丸事件”的前一个事件，错误定位与重试都以
	// 单事件为粒度；任何已返回成功的事件都不会因后续失败而被重复处理。
	processed := 0
	for _, e := range decoded {
		if err := p.applyOne(ctx, e); err != nil {
			_ = p.recordFailure(ctx, e, err)
			return processed, fmt.Errorf("projection %q: apply event seq=%d type=%s stream=%s: %w",
				p.cfg.Name, e.Envelope.GlobalSequence, e.Envelope.Type, e.Envelope.Stream, err)
		}
		newSeq := e.Envelope.GlobalSequence
		if err := p.cfg.Checkpoints.Save(ctx, Checkpoint{
			Projection: p.cfg.Name,
			Sequence:   newSeq,
			UpdatedAt:  nowRFC3339(),
		}); err != nil {
			return processed, err
		}
		processed++
	}
	if p.cfg.Failures != nil {
		if f, _ := p.cfg.Failures.Load(ctx, p.cfg.Name); f != nil && f.Sequence <= cp.Sequence+int64(processed) {
			_ = p.cfg.Failures.Clear(ctx, p.cfg.Name)
		}
	}
	return processed, nil
}

// applyOne 应用单个事件；若读模型实现了 Batcher，则以单元素批次交付，
// 保持“读模型只感知一种入口”。逐事件推进检查点与 Batcher 并不矛盾：
// 批次仅用于减少调用开销，原子性语义仍由检查点保证。
func (p *Projector) applyOne(ctx context.Context, e DecodedEvent) error {
	if b, ok := p.cfg.Model.(Batcher); ok {
		return b.ApplyBatch(ctx, []DecodedEvent{e})
	}
	return p.cfg.Model.Apply(ctx, e)
}

// prepare 对原始批次做三道保险，保证投递给读模型的事件严格有序、不重复：
//  1. 丢弃 GlobalSequence <= checkpoint 的重复/回退事件（重复投递、乱序唤醒）；
//  2. 按 GlobalSequence 升序排序（容忍乱序到达）；
//  3. 逐个经过注册表升级到当前 schema 后再解码。
func (p *Projector) prepare(raw []eventstore.RecordedEvent, checkpoint int64) ([]DecodedEvent, error) {
	seen := make(map[int64]struct{}, len(raw))
	filtered := raw[:0:len(raw)]
	for _, e := range raw {
		if e.GlobalSequence <= checkpoint {
			continue
		}
		if _, dup := seen[e.GlobalSequence]; dup {
			continue
		}
		seen[e.GlobalSequence] = struct{}{}
		filtered = append(filtered, e)
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].GlobalSequence < filtered[j].GlobalSequence
	})
	out := make([]DecodedEvent, 0, len(filtered))
	for _, e := range filtered {
		v, from, err := p.cfg.Registry.Decode(e.Type, e.Data)
		if err != nil {
			return nil, fmt.Errorf("projection %q: upgrade event seq=%d type=%s: %w",
				p.cfg.Name, e.GlobalSequence, e.Type, err)
		}
		out = append(out, DecodedEvent{Event: v, Envelope: e, OriginalVersion: from})
	}
	return out, nil
}

func (p *Projector) applyBatch(ctx context.Context, events []DecodedEvent) error {
	if b, ok := p.cfg.Model.(Batcher); ok {
		return b.ApplyBatch(ctx, events)
	}
	for _, e := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := p.cfg.Model.Apply(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

func (p *Projector) recordFailure(ctx context.Context, e DecodedEvent, cause error) error {
	if p.cfg.Failures == nil {
		return nil
	}
	now := nowRFC3339()
	return p.cfg.Failures.Save(ctx, &Failure{
		Projection:      p.cfg.Name,
		Sequence:        e.Envelope.GlobalSequence,
		Stream:          e.Envelope.Stream,
		StreamVersion:   e.Envelope.Version,
		EventType:       e.Envelope.Type,
		EventID:         e.Envelope.ID,
		Message:         cause.Error(),
		FirstOccurredAt: now,
		LastOccurredAt:  now,
	})
}

// CatchUp 反复 ProcessOnce 直到追平存储末尾或出错。
// 适用于启动时恢复：从检查点继续，绝不重放已确认事件。
func (p *Projector) CatchUp(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := p.ProcessOnce(ctx)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

// Rebuild 在【不阻塞事件写入】的前提下从完整历史重建读模型。
//
// 过程：
//  1. 订阅“重建期间到达的新事件”（先挂订阅，再读起点，杜绝通知缺口）；
//  2. 清空读模型（Reset）、清零检查点、清除历史失败记录；
//  3. 从全局序号 0 开始按批重放全部历史；
//  4. 进入追赶循环，消费重建期间并发写入的新事件直到追平。
//
// 整个过程中 Store.Append 始终可用：写入照常提交，投影通过订阅+轮询
// 最终收敛。读模型在重建期间处于中间态，不应被外部查询（或由调用方
// 通过双缓冲/交换读视图隔离——Reset/Build/Swap 是读模型自己的选择）。
func (p *Projector) Rebuild(ctx context.Context) error {
	p.runMu.Lock()
	if p.running {
		p.runMu.Unlock()
		return errors.New("projection: cannot rebuild while Run loop is active")
	}
	p.runMu.Unlock()

	last, err := p.cfg.Store.LastSequence(ctx)
	if err != nil {
		return err
	}
	sub := p.cfg.Store.Subscribe(0)
	defer sub.Close()

	if err := p.resetModel(ctx); err != nil {
		return err
	}
	if err := p.cfg.Checkpoints.Reset(ctx, p.cfg.Name); err != nil {
		return err
	}
	if p.cfg.Failures != nil {
		_ = p.cfg.Failures.Clear(ctx, p.cfg.Name)
	}

	// 历史重放（截至重建开始时的末尾）。
	if err := p.replayRange(ctx, 0, last); err != nil {
		return err
	}
	// 追赶重建期间的新写入，直到连续两轮没有新事件。
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		cur, err := p.cfg.Store.LastSequence(ctx)
		if err != nil {
			return err
		}
		cp, err := p.cfg.Checkpoints.Load(ctx, p.cfg.Name)
		if err != nil {
			return err
		}
		if cp.Sequence >= cur {
			// 等待期间可能正好有写入：非阻塞排空一次信号再确认。
			select {
			case <-sub.C():
				continue
			default:
			}
			cur2, _ := p.cfg.Store.LastSequence(ctx)
			if cur2 <= cp.Sequence {
				p.log.Info("projection rebuilt", "name", p.cfg.Name, "sequence", cp.Sequence)
				return nil
			}
		}
		if err := p.CatchUp(ctx); err != nil {
			return err
		}
	}
}

// replayRange 从 afterSeq 重放到 upTo（含），逐批推进检查点。
func (p *Projector) replayRange(ctx context.Context, afterSeq, upTo int64) error {
	for afterSeq < upTo {
		end := upTo
		if end-afterSeq > p.batchSize {
			end = afterSeq + p.batchSize
		}
		raw, err := p.cfg.Store.ReadAll(ctx, afterSeq, end)
		if err != nil {
			return err
		}
		if len(raw) == 0 {
			return fmt.Errorf("projection %q: event gap after sequence %d", p.cfg.Name, afterSeq)
		}
		decoded, err := p.prepare(raw, afterSeq)
		if err != nil {
			return err
		}
		if err := p.applyBatch(ctx, decoded); err != nil {
			failed := decoded[0]
			_ = p.recordFailure(ctx, failed, err)
			return fmt.Errorf("projection %q: rebuild apply seq=%d: %w",
				p.cfg.Name, failed.Envelope.GlobalSequence, err)
		}
		newSeq := decoded[len(decoded)-1].Envelope.GlobalSequence
		if err := p.cfg.Checkpoints.Save(ctx, Checkpoint{
			Projection: p.cfg.Name,
			Sequence:   newSeq,
			UpdatedAt:  nowRFC3339(),
		}); err != nil {
			return err
		}
		afterSeq = newSeq
	}
	return nil
}

func (p *Projector) resetModel(ctx context.Context) error {
	r, ok := p.cfg.Model.(Resettable)
	if !ok {
		return fmt.Errorf("projection %q: model %T does not implement Resettable, cannot rebuild",
			p.cfg.Name, p.cfg.Model)
	}
	return r.Reset(ctx)
}

// Resettable 由支持重建的读模型实现：清空所有状态。
type Resettable interface {
	Reset(ctx context.Context) error
}

// Run 启动增量循环直到 ctx 取消。内部处理订阅唤醒与兜底轮询；
// 处理失败时按指数退避（2x，上限 5s）重试，检查点不动，事件不丢。
//
// 同一 Projector 的 Run 不可重入；Run 期间也不能 Rebuild。
func (p *Projector) Run(ctx context.Context) error {
	p.runMu.Lock()
	if p.running {
		p.runMu.Unlock()
		return errors.New("projection: Run already active")
	}
	p.running = true
	p.sub = p.cfg.Store.Subscribe(0)
	p.runMu.Unlock()
	defer func() {
		p.runMu.Lock()
		p.sub.Close()
		p.sub = nil
		p.running = false
		p.runMu.Unlock()
	}()

	backoff := p.poll
	maxBackoff := 5 * time.Second
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.sub.C():
		case <-timer.C:
		}
		n, err := p.ProcessOnce(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			p.log.Warn("projection iteration failed; will retry",
				"name", p.cfg.Name, "error", err.Error(), "backoff", backoff.String())
			timer.Reset(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}
		backoff = p.poll
		if n == 0 {
			timer.Reset(p.poll)
		} else {
			timer.Reset(0)
		}
	}
}

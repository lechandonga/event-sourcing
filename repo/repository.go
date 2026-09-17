// Package repo 提供事件溯源聚合的通用仓储：
// 乐观并发保存、按流重放、快照加速，以及“快照缺失/过期/损坏/超前”时
// 一律安全回退到顺序完整重放的恢复路径。
package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/lechandonga/event-sourcing/event"
	"github.com/lechandonga/event-sourcing/eventstore"
	"github.com/lechandonga/event-sourcing/projection"
	"github.com/lechandonga/event-sourcing/snapshot"
)

// Aggregate 是仓储可管理的聚合根指针约束：
// A 必须是 *S，并实现一组 XxxAccessor 方法。
// Apply 接收的是“已经升级到当前 schema”的事件；聚合自身不需要关心历史结构，
// 跨版本兼容由事件注册表与投影仪式解码统一完成。
type Aggregate[A any] interface {
	*A
	IDAccessor() string
	VersionAccessor() int64
	PendingAccessor() []any
	CommitAccessor(int64)
	ApplyAccessor(projection.DecodedEvent) error
}

// Repository 是泛型聚合仓储。A 是聚合指针类型，S 是聚合值类型。
type Repository[A Aggregate[S], S any] struct {
	streamType string
	store      eventstore.Store
	registry   *event.Registry
	snapshots  snapshot.Store
	codec      SnapshotCodec[A]
	threshold  int64 // 自上次快照以来新增多少事件后再打快照；0 表示关闭自动快照
	log        *slog.Logger
	// OnFallback 在快照回退（过期/损坏/超前）时被调用，便于监控与测试。
	// 原因取值："corrupt" | "stale" | "ahead" | "gap" | "identity"。
	OnFallback func(reason string, id string, detail string)
}

// SnapshotCodec 负责聚合与快照负载之间的序列化，以及从快照恢复聚合。
type SnapshotCodec[A any] interface {
	// Encode 返回当前聚合的快照负载 JSON。
	Encode(agg A) (json.RawMessage, error)
	// Restore 把快照负载应用到聚合（通常 agg 为零值指针）。
	Restore(agg A, payload json.RawMessage, snapVersion int64) error
}

// Option 配置仓储。
type Option[A any] func(*config[A])

type config[A any] struct {
	snapshots  snapshot.Store
	codec      SnapshotCodec[A]
	threshold  int64
	log        *slog.Logger
	onFallback func(reason, id, detail string)
}

// WithSnapshots 启用快照：指定存储、编解码与重放事件数阈值（达到即重打快照）。
func WithSnapshots[A any](store snapshot.Store, codec SnapshotCodec[A], threshold int64) Option[A] {
	return func(c *config[A]) {
		c.snapshots = store
		c.codec = codec
		c.threshold = threshold
	}
}

// WithLogger 注入日志。
func WithLogger[A any](log *slog.Logger) Option[A] {
	return func(c *config[A]) { c.log = log }
}

// WithFallbackHook 注入快照回退观测钩子。
func WithFallbackHook[A any](fn func(reason, id, detail string)) Option[A] {
	return func(c *config[A]) { c.onFallback = fn }
}

// New 创建仓储。streamType 是聚合类型（流名前缀）。
func New[A Aggregate[S], S any](
	streamType string,
	st eventstore.Store,
	registry *event.Registry,
	opts ...Option[A],
) *Repository[A, S] {
	cfg := &config[A]{threshold: 0}
	for _, o := range opts {
		o(cfg)
	}
	log := cfg.log
	if log == nil {
		log = slog.Default()
	}
	return &Repository[A, S]{
		streamType: streamType,
		store:      st,
		registry:   registry,
		snapshots:  cfg.snapshots,
		codec:      cfg.codec,
		threshold:  cfg.threshold,
		log:        log,
		OnFallback: cfg.onFallback,
	}
}

// Save 保存聚合的待提交事件。
//
// 并发控制：以聚合“保存前已提交版本”作为期望版本交给事件存储；
// 版本冲突时整批拒绝、不产生任何已提交事件，返回 *eventstore.ConflictError，
// 调用方可读取 .Reason 区分原因并重试（重新 Load -> 重放命令 -> Save）。
func (r *Repository[A, S]) Save(ctx context.Context, agg A) (int64, error) {
	pending := agg.PendingAccessor()
	if len(pending) == 0 {
		return agg.VersionAccessor(), nil
	}
	events := make([]eventstore.UncommittedEvent, 0, len(pending))
	for _, p := range pending {
		v, ok := p.(event.Versioned)
		if !ok {
			return 0, fmt.Errorf("repo: pending event %T does not implement event.Versioned", p)
		}
		named, ok := p.(interface{ EventType() string })
		if !ok {
			return 0, fmt.Errorf("repo: pending event %T has no EventType()", p)
		}
		raw, err := event.Encode(v)
		if err != nil {
			return 0, fmt.Errorf("repo: encode %s: %w", named.EventType(), err)
		}
		events = append(events, eventstore.UncommittedEvent{Type: named.EventType(), Data: raw})
	}

	stream := eventstore.StreamID{Type: r.streamType, ID: agg.IDAccessor()}
	expected := eventstore.ExpectedVersion(agg.VersionAccessor())
	committed, err := r.store.Append(ctx, stream, expected, events)
	if err != nil {
		return 0, err
	}
	newVersion := committed[len(committed)-1].Version
	agg.CommitAccessor(newVersion)

	if r.snapshots != nil && r.threshold > 0 {
		// 阈值以版本近似：每次保存后若版本跨过阈值边界就重打。
		// 更精确的“自上次快照事件数”在 Load 时通过快照版本对齐，这里只需
		// 在新版本是 threshold 的整数倍时落快照即可（可证明两次快照间隔 <= 2*threshold）。
		if newVersion/r.threshold > (newVersion-int64(len(committed)))/r.threshold {
			r.saveSnapshot(ctx, agg, committed[len(committed)-1])
		}
	}
	return newVersion, nil
}

func (r *Repository[A, S]) saveSnapshot(ctx context.Context, agg A, last eventstore.RecordedEvent) {
	if r.codec == nil {
		return
	}
	payload, err := r.codec.Encode(agg)
	if err != nil {
		r.log.Warn("snapshot encode failed", "stream", last.Stream, "error", err.Error())
		return
	}
	snap := &snapshot.Snapshot{
		StreamType:     r.streamType,
		StreamID:       agg.IDAccessor(),
		Version:        last.Version,
		GlobalSequence: last.GlobalSequence,
		Payload:        payload,
		CreatedAt:      time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := snap.Seal(); err != nil {
		r.log.Warn("snapshot seal failed", "stream", last.Stream, "error", err.Error())
		return
	}
	if err := r.snapshots.Save(ctx, snap); err != nil {
		// 快照失败不影响已提交事实：下次加载会回退到重放。
		r.log.Warn("snapshot save failed", "stream", last.Stream, "error", err.Error())
	}
}

// Load 按 ID 加载聚合：先尝试快照，再从快照版本之后顺序重放。
//
// 任何快照异常都回退到“空聚合 + 从版本 0 完整重放”，绝不跳过事件：
//   - 快照缺失：从头重放（正常冷路径）；
//   - 校验和/JSON 损坏：删除坏快照、记录回调、从头重放；
//   - 快照过期（版本落后于流）：从 Version+1 重放（常规加速路径）；
//   - 快照超前（版本大于流末尾）：数据不一致，删除快照、从头重放；
//   - 事件区间出现空洞：回退从头重放（防御性，正常存储不会发生）。
func (r *Repository[A, S]) Load(ctx context.Context, id string) (A, int64, error) {
	agg := newAggregate[A, S]()
	stream := eventstore.StreamID{Type: r.streamType, ID: id}

	streamVer, err := r.store.StreamVersion(ctx, stream)
	if err != nil {
		if errors.Is(err, eventstore.ErrStreamNotFound) {
			return agg, 0, ErrNotFound
		}
		return agg, 0, err
	}

	fromVersion := int64(0)
	if r.snapshots != nil && r.codec != nil {
		fromVersion = r.trySnapshot(ctx, agg, id)
	}

	if fromVersion > streamVer {
		// 快照超前：不可信任，回退。
		r.fallback("ahead", id,
			"snapshot version "+strconv.FormatInt(fromVersion, 10)+" > stream "+strconv.FormatInt(streamVer, 10))
		if r.snapshots != nil {
			_ = r.snapshots.Delete(ctx, r.streamType, id)
		}
		agg = newAggregate[A, S]()
		fromVersion = 0
	}

	events, err := r.store.ReadStream(ctx, stream, fromVersion, streamVer)
	if err != nil {
		return agg, fromVersion, err
	}
	// 版本连续性校验：任何空洞都意味着状态不完整，回退全量重放。
	if err := checkContiguous(events, fromVersion); err != nil {
		r.fallback("gap", id, err.Error())
		if r.snapshots != nil {
			_ = r.snapshots.Delete(ctx, r.streamType, id)
		}
		agg = newAggregate[A, S]()
		events, err = r.store.ReadStream(ctx, stream, 0, streamVer)
		if err != nil {
			return agg, 0, err
		}
		if err := checkContiguous(events, 0); err != nil {
			return agg, 0, err // 事件流本身损坏，无法安全加载
		}
	}
	for _, e := range events {
		decoded, err := r.decode(e)
		if err != nil {
			return agg, fromVersion, err
		}
		if err := agg.ApplyAccessor(decoded); err != nil {
			return agg, fromVersion, err
		}
	}
	return agg, streamVer, nil
}

// trySnapshot 尝试加载并应用快照，返回“应从该版本之后开始重放”的版本号。
// 任何异常都返回 0（调用方据此全量重放）。
func (r *Repository[A, S]) trySnapshot(ctx context.Context, agg A, id string) int64 {
	snap, err := r.snapshots.Load(ctx, r.streamType, id)
	if err != nil {
		if errors.Is(err, snapshot.ErrSnapshotNotFound) {
			return 0
		}
		// 损坏：清理坏文件，保证下次不再读到。
		r.fallback("corrupt", id, err.Error())
		_ = r.snapshots.Delete(ctx, r.streamType, id)
		return 0
	}
	if snap.StreamType != r.streamType || snap.StreamID != id {
		r.fallback("identity", id, "snapshot identity mismatch")
		_ = r.snapshots.Delete(ctx, r.streamType, id)
		return 0
	}
	if snap.Version < 0 {
		r.fallback("corrupt", id, "negative snapshot version")
		_ = r.snapshots.Delete(ctx, r.streamType, id)
		return 0
	}
	if err := r.codec.Restore(agg, snap.Payload, snap.Version); err != nil {
		r.fallback("corrupt", id, "restore payload: "+err.Error())
		_ = r.snapshots.Delete(ctx, r.streamType, id)
		return 0
	}
	// 快照恢复后必须把聚合版本对齐到快照版本（Apply 剩余事件时会再被推进）。
	agg.CommitAccessor(snap.Version)
	// “过期”不是异常：从 Version+1 继续即可，不触发回调（只有不寻常的回退才需要告警）。
	return snap.Version
}

func (r *Repository[A, S]) decode(e eventstore.RecordedEvent) (projection.DecodedEvent, error) {
	v, from, err := r.registry.Decode(e.Type, e.Data)
	if err != nil {
		return projection.DecodedEvent{}, fmt.Errorf("repo: upgrade %s: %w", e.Type, err)
	}
	return projection.DecodedEvent{Event: v, Envelope: e, OriginalVersion: from}, nil
}

func checkContiguous(events []eventstore.RecordedEvent, fromVersion int64) error {
	want := fromVersion + 1
	for _, e := range events {
		if e.Version != want {
			return fmt.Errorf("event version gap: want %d got %d", want, e.Version)
		}
		want++
	}
	return nil
}

func (r *Repository[A, S]) fallback(reason, id, detail string) {
	if r.OnFallback != nil {
		r.OnFallback(reason, id, detail)
	}
	r.log.Info("snapshot fallback to full replay",
		"reason", reason, "streamType", r.streamType, "id", id, "detail", detail)
}

// newAggregate 以类型安全的方式构造零值聚合指针。
// 约束保证 A 的底层类型就是 *S，因此 any(new(S)).(A) 在运行时必然成立。
func newAggregate[A Aggregate[S], S any]() A {
	var zero S
	return any(&zero).(A)
}

// ErrNotFound 表示聚合不存在。
var ErrNotFound = errors.New("repo: aggregate not found")

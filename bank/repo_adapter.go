package bank

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/lechandonga/event-sourcing/event"
	"github.com/lechandonga/event-sourcing/eventstore"
	"github.com/lechandonga/event-sourcing/projection"
	"github.com/lechandonga/event-sourcing/repo"
	"github.com/lechandonga/event-sourcing/snapshot"
)

// 下面这组 XxxAccessor 方法把 Account 适配到 repo.Aggregate 约束。
// 命名带后缀以区别于领域方法 ID()/Version()，它们是基础设施接缝。

func (a *Account) IDAccessor() string     { return a.id }
func (a *Account) VersionAccessor() int64 { return a.version }
func (a *Account) PendingAccessor() []any { return a.PendingEvents() }
func (a *Account) CommitAccessor(v int64) { a.CommitEvents(v) }
func (a *Account) ApplyAccessor(e projection.DecodedEvent) error {
	return a.Apply(e)
}

// AccountCodec 实现 repo.SnapshotCodec[*Account]。
type AccountCodec struct{}

// Encode 序列化聚合快照负载。
func (AccountCodec) Encode(a *Account) (json.RawMessage, error) {
	return json.Marshal(a.SnapshotState())
}

// Restore 从快照负载恢复聚合。
func (AccountCodec) Restore(a *Account, payload json.RawMessage, _ int64) error {
	var st AccountState
	if err := json.Unmarshal(payload, &st); err != nil {
		return err
	}
	a.RestoreState(st)
	return nil
}

// AccountRepositoryConfig 是账户仓储的构造配置。
type AccountRepositoryConfig struct {
	Store     eventstore.Store
	Registry  *event.Registry
	Snapshots snapshot.Store // 可选；nil 关闭快照
	Codec     repo.SnapshotCodec[*Account]
	Threshold int64 // 触发重打快照的版本间隔；0 关闭自动快照
	Logger    *slog.Logger
	// OnFallback 在快照回退（corrupt/ahead/gap/identity）时被调用。
	OnFallback func(reason, id, detail string)
}

// NewAccountRepository 构造账户聚合仓储。
func NewAccountRepository(cfg AccountRepositoryConfig) *repo.Repository[*Account, Account] {
	var opts []repo.Option[*Account]
	if cfg.Snapshots != nil {
		codec := cfg.Codec
		if codec == nil {
			codec = AccountCodec{}
		}
		threshold := cfg.Threshold
		if threshold <= 0 {
			threshold = 50
		}
		opts = append(opts, repo.WithSnapshots[*Account](cfg.Snapshots, codec, threshold))
	}
	if cfg.Logger != nil {
		opts = append(opts, repo.WithLogger[*Account](cfg.Logger))
	}
	if cfg.OnFallback != nil {
		opts = append(opts, repo.WithFallbackHook[*Account](cfg.OnFallback))
	}
	registry := cfg.Registry
	if registry == nil {
		registry = NewRegistry()
	}
	return repo.New[*Account, Account](AccountType, cfg.Store, registry, opts...)
}

// SaveAccount 是保存账户的便捷包装。
func SaveAccount(ctx context.Context, r *repo.Repository[*Account, Account], a *Account) (int64, error) {
	return r.Save(ctx, a)
}

// LoadAccount 是加载账户的便捷包装。
func LoadAccount(ctx context.Context, r *repo.Repository[*Account, Account], id string) (*Account, error) {
	a, _, err := r.Load(ctx, id)
	return a, err
}

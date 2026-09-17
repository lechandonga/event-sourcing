package eventstore

import (
	"context"
	"encoding/json"
	"time"
)

// ExpectedVersion 用于乐观并发控制。
//
// 语义：
//   - 期望值等于流当前版本时，追加才会生效（版本从 0 开始计数）；
//   - ExpectedVersionNew(0)：流必须不存在（首次创建聚合）；
//   - ExpectedVersionAny(-1)：不做版本校验（不推荐，通常用于迁移/导入）。
type ExpectedVersion int64

const (
	// ExpectedVersionNew 表示流必须尚不存在（期望版本为 0）。
	ExpectedVersionNew ExpectedVersion = 0
	// ExpectedVersionAny 跳过乐观并发校验。
	ExpectedVersionAny ExpectedVersion = -1
)

// UncommittedEvent 是尚未提交的事件数据。
// ID 留空时由存储分配。Version 由存储根据流上一版本分配，调用方无需填写。
type UncommittedEvent struct {
	ID       string          `json:"id,omitempty"`
	Type     string          `json:"type"`
	Data     json.RawMessage `json:"data"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

// RecordedEvent 是已提交到事件流的不可变事实。
// Version 在该聚合流内单调递增（从 1 开始）；
// GlobalSequence 在整个存储内全局唯一、严格递增，可用于全局订阅顺序。
type RecordedEvent struct {
	GlobalSequence int64           `json:"globalSequence"`
	Stream         string          `json:"stream"`
	StreamType     string          `json:"streamType"`
	Version        int64           `json:"version"`
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Time           time.Time       `json:"time"`
	Data           json.RawMessage `json:"data"`
	Metadata       json.RawMessage `json:"metadata,omitempty"`
}

// StreamID 是一个聚合流的标识。
type StreamID struct {
	// Type 是聚合类型，例如 "bank.Account"。同一 ID 不同类型视为不同的流，
	// 但同名字符串的聚合类型不一致会被拒绝（防止标识碰撞）。
	Type string
	// ID 是聚合实例 ID。
	ID string
}

// StreamName 返回流的规范名称："<type>-<id>"。
func (s StreamID) StreamName() string { return s.Type + "-" + s.ID }

// Store 是只追加的事件存储。所有实现必须对并发追加安全。
type Store interface {
	// Append 把一组事件原子追加到某条聚合流。
	//
	// 同一次调用中的事件按顺序分配连续版本；任一字段非法或版本冲突时整批拒绝，
	// 不得有任何事件进入已提交流。
	Append(ctx context.Context, stream StreamID, expected ExpectedVersion, events []UncommittedEvent) ([]RecordedEvent, error)

	// ReadStream 读取流中版本区间 (fromVersion, toVersion] 的事件。
	// toVersion<=0 表示读到流末尾。流不存在返回 ErrStreamNotFound。
	ReadStream(ctx context.Context, stream StreamID, fromVersion, toVersion int64) ([]RecordedEvent, error)

	// StreamVersion 返回流当前版本；流不存在返回 0 与 ErrStreamNotFound。
	StreamVersion(ctx context.Context, stream StreamID) (int64, error)

	// ReadAll 读取全局序号区间 (afterSequence, upToSequence] 的事件（按全局序号升序）。
	// upToSequence<=0 表示读到当前末尾。
	ReadAll(ctx context.Context, afterSequence, upToSequence int64) ([]RecordedEvent, error)

	// LastSequence 返回存储当前最大全局序号（空存储为 0）。
	LastSequence(ctx context.Context) (int64, error)

	// Subscribe 从 afterSequence（不含）之后订阅新提交的事件。
	// 订阅不回放历史（请先用 ReadAll 追平），仅作为“有新写入”的唤醒信号；
	// 消费者必须仍以 ReadAll 拉取到的事件为准（通知可能合并/重复，幂等安全）。
	Subscribe(afterSequence int64) Subscription

	// Close 释放资源。
	Close(ctx context.Context) error
}

// Subscription 是事件订阅句柄。
type Subscription interface {
	// C 在有新提交事件时收到唤醒通知（通知不带事件、可能合并）。
	C() <-chan struct{}
	// Close 取消订阅。
	Close()
}

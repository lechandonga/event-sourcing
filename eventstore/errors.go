package eventstore

import (
	"errors"
	"fmt"
)

// ConflictReason 是乐观并发冲突的可区分原因。
type ConflictReason string

const (
	// ReasonVersionConflict 流已存在且版本与期望值不符（并发写竞争）。
	ReasonVersionConflict ConflictReason = "version-conflict"
	// ReasonStreamExists 期望创建新流，但流已经存在。
	ReasonStreamExists ConflictReason = "stream-exists"
	// ReasonStreamNotFound 期望流存在（版本>=0），但流不存在。
	ReasonStreamNotFound ConflictReason = "stream-not-found"
	// ReasonAggregateTypeMismatch 同一流名此前以另一种聚合类型写入过。
	ReasonAggregateTypeMismatch ConflictReason = "aggregate-type-mismatch"
)

var (
	// ErrVersionConflict 版本冲突；可用 errors.Is 判断，原因用 ConflictError.Reason 区分。
	ErrVersionConflict = errors.New("eventstore: optimistic concurrency conflict")
	// ErrStreamNotFound 流不存在。
	ErrStreamNotFound = errors.New("eventstore: stream not found")
	// ErrInvalidEvent 追加的事件非法（空类型、空负载等）。
	ErrInvalidEvent = errors.New("eventstore: invalid event")
	// ErrEmptyBatch 追加批次为空。
	ErrEmptyBatch = errors.New("eventstore: empty event batch")
	// ErrClosed 存储已关闭。
	ErrClosed = errors.New("eventstore: store is closed")
	// ErrCorrupt 持久化数据损坏。
	ErrCorrupt = errors.New("eventstore: corrupt event log")
)

// ConflictError 携带可区分的冲突原因，同时支持 errors.Is(err, ErrVersionConflict)。
type ConflictError struct {
	Reason   ConflictReason
	Stream   string
	Expected int64
	Actual   int64
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("eventstore: concurrency conflict on %s: %s (expected version %d, actual %d)",
		e.Stream, e.Reason, e.Expected, e.Actual)
}

// Is 使 ConflictError 可通过 errors.Is 匹配 ErrVersionConflict。
func (e *ConflictError) Is(target error) bool { return target == ErrVersionConflict }

func newConflict(reason ConflictReason, stream string, expected, actual int64) error {
	return &ConflictError{Reason: reason, Stream: stream, Expected: expected, Actual: actual}
}

package eventstore

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryStore 是纯进程内事件存储。零值不可用，请用 NewMemoryStore 构造。
//
// 事件保存在两个视图中：全局有序切片 all（权威顺序）与按流索引。
// 同一把互斥锁保护所有结构，保证追加的原子性与读一致性。
type MemoryStore struct {
	mu          sync.Mutex
	streams     map[string][]RecordedEvent // stream name -> 已提交事件（按 version 升序）
	streamTypes map[string]string          // stream name -> 聚合类型
	all         []RecordedEvent            // 按 globalSequence 升序
	lastSeq     int64
	subs        map[int64]map[int64]chan struct{}
	nextSubID   int64
	closed      bool
	now         func() time.Time
}

// NewMemoryStore 创建内存事件存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		streams:     make(map[string][]RecordedEvent),
		streamTypes: make(map[string]string),
		subs:        make(map[int64]map[int64]chan struct{}),
		now:         time.Now,
	}
}

// validateLocked 在持锁状态下做全部写入前校验，并返回流当前版本。
func (m *MemoryStore) validateLocked(stream StreamID, expected ExpectedVersion, events []UncommittedEvent) (int64, error) {
	if m.closed {
		return 0, ErrClosed
	}
	name := stream.StreamName()
	current := int64(len(m.streams[name]))

	if existingType, ok := m.streamTypes[name]; ok && existingType != stream.Type {
		return current, newConflict(ReasonAggregateTypeMismatch, name, int64(expected), current)
	}
	if len(events) == 0 {
		return current, ErrEmptyBatch
	}
	for _, e := range events {
		if e.Type == "" || len(e.Data) == 0 {
			return current, ErrInvalidEvent
		}
	}
	switch {
	case expected == ExpectedVersionAny:
		// 跳过版本校验
	case expected == ExpectedVersionNew && current > 0:
		return current, newConflict(ReasonStreamExists, name, int64(expected), current)
	case expected > ExpectedVersionNew && current == 0:
		return current, newConflict(ReasonStreamNotFound, name, int64(expected), current)
	case expected >= ExpectedVersionNew && int64(expected) != current:
		return current, newConflict(ReasonVersionConflict, name, int64(expected), current)
	case expected < ExpectedVersionAny:
		return current, ErrInvalidEvent
	}
	return current, nil
}

// buildLocked 在持锁状态下基于当前 lastSeq 分配序号/版本，构造待提交记录。
// 调用方必须已经通过 validateLocked。记录尚未写入任何视图。
func (m *MemoryStore) buildLocked(stream StreamID, current int64, events []UncommittedEvent) []RecordedEvent {
	now := m.now().UTC()
	committed := make([]RecordedEvent, len(events))
	for i, e := range events {
		id := e.ID
		if id == "" {
			id = newEventID()
		}
		committed[i] = RecordedEvent{
			GlobalSequence: m.lastSeq + int64(i) + 1,
			Stream:         stream.StreamName(),
			StreamType:     stream.Type,
			Version:        current + int64(i) + 1,
			ID:             id,
			Type:           e.Type,
			Time:           now,
			Data:           copyBytes(e.Data),
			Metadata:       copyBytes(e.Metadata),
		}
	}
	return committed
}

// commitLocked 在持锁状态下复检版本并提交。
//
// 复检保证：即便“构造记录”和“提交”之间释放过锁（FileStore 两阶段提交
// 必须释放锁以做磁盘 I/O），并发写入也无法造成版本或全局序号重复——
// 一旦首版本与当前流尾不连续，返回 conflict 让调用方回滚外部副作用。
func (m *MemoryStore) commitLocked(stream StreamID, prepared []RecordedEvent) ([]RecordedEvent, error) {
	name := stream.StreamName()
	current := int64(len(m.streams[name]))
	first := prepared[0]
	if first.Version != current+1 {
		return nil, newConflict(ReasonVersionConflict, name, current, first.Version-1)
	}
	if first.GlobalSequence != m.lastSeq+1 {
		// 全局序号被其它提交抢先：直接拒绝。整批语义保证不会部分生效。
		return nil, newConflict(ReasonVersionConflict, name, current, first.Version-1)
	}
	if existingType, ok := m.streamTypes[name]; ok && existingType != stream.Type {
		return nil, newConflict(ReasonAggregateTypeMismatch, name, current, first.Version-1)
	}
	m.streams[name] = append(m.streams[name], prepared...)
	m.streamTypes[name] = stream.Type
	m.all = append(m.all, prepared...)
	m.lastSeq = prepared[len(prepared)-1].GlobalSequence
	m.notifyLocked()
	return prepared, nil
}

// prepareForDisk 供 FileStore 使用：持锁校验并构造记录后返回。
// 调用方拿到记录后必须最终调用 commitForDisk 或放弃；二者之间允许释放锁做 I/O。
func (m *MemoryStore) prepareForDisk(stream StreamID, expected ExpectedVersion, events []UncommittedEvent) ([]RecordedEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, err := m.validateLocked(stream, expected, events)
	if err != nil {
		return nil, err
	}
	return m.buildLocked(stream, current, events), nil
}

// commitForDisk 供 FileStore 在磁盘写入成功后复检提交；冲突时返回错误，
// 由调用方负责截断刚刚写入的半批记录。
func (m *MemoryStore) commitForDisk(stream StreamID, prepared []RecordedEvent) ([]RecordedEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commitLocked(stream, prepared)
}

// ImportBatch 供 FileStore 启动加载时调用：把一批已经过逐事件连续性校验的
// 历史记录并入内存视图。历史数据是权威事实，不做乐观版本复检；按批次的
// 全局序号必须严格接续当前 lastSeq（FileStore 已按文件顺序验证）。
func (m *MemoryStore) ImportBatch(stream StreamID, batch []RecordedEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	if len(batch) == 0 {
		return ErrEmptyBatch
	}
	name := stream.StreamName()
	if t, ok := m.streamTypes[name]; ok && t != stream.Type {
		return newConflict(ReasonAggregateTypeMismatch, name, 0, int64(len(m.streams[name])))
	}
	if batch[0].GlobalSequence != m.lastSeq+1 {
		return fmt.Errorf("eventstore: import batch not contiguous: want seq %d got %d: %w",
			m.lastSeq+1, batch[0].GlobalSequence, ErrCorrupt)
	}
	m.streams[name] = append(m.streams[name], batch...)
	m.streamTypes[name] = stream.Type
	m.all = append(m.all, batch...)
	m.lastSeq = batch[len(batch)-1].GlobalSequence
	return nil
}

// Append 原子追加一批事件（校验、构造、提交在同一锁内完成）。
func (m *MemoryStore) Append(_ context.Context, stream StreamID, expected ExpectedVersion, events []UncommittedEvent) ([]RecordedEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, err := m.validateLocked(stream, expected, events)
	if err != nil {
		return nil, err
	}
	prepared := m.buildLocked(stream, current, events)
	return m.commitLocked(stream, prepared)
}

// ReadStream 读取版本区间 (fromVersion, toVersion]。
func (m *MemoryStore) ReadStream(_ context.Context, stream StreamID, fromVersion, toVersion int64) ([]RecordedEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	events, ok := m.streams[stream.StreamName()]
	if !ok {
		return nil, ErrStreamNotFound
	}
	if fromVersion < 0 {
		fromVersion = 0
	}
	if toVersion <= 0 || toVersion > int64(len(events)) {
		toVersion = int64(len(events))
	}
	if fromVersion >= toVersion {
		return []RecordedEvent{}, nil
	}
	return cloneEvents(events[fromVersion:toVersion]), nil
}

// StreamVersion 返回流当前版本。
func (m *MemoryStore) StreamVersion(_ context.Context, stream StreamID) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	events, ok := m.streams[stream.StreamName()]
	if !ok {
		return 0, ErrStreamNotFound
	}
	return int64(len(events)), nil
}

// ReadAll 读取全局序号区间 (afterSequence, upToSequence]。
func (m *MemoryStore) ReadAll(_ context.Context, afterSequence, upToSequence int64) ([]RecordedEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if afterSequence < 0 {
		afterSequence = 0
	}
	if afterSequence >= m.lastSeq {
		return []RecordedEvent{}, nil
	}
	end := m.lastSeq
	if upToSequence > 0 && upToSequence < end {
		end = upToSequence
	}
	if afterSequence >= end {
		return []RecordedEvent{}, nil
	}
	return cloneEvents(m.all[afterSequence:end]), nil
}

// LastSequence 返回当前最大全局序号。
func (m *MemoryStore) LastSequence(_ context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastSeq, nil
}

// Subscribe 返回一个唤醒型订阅。
func (m *MemoryStore) Subscribe(afterSequence int64) Subscription {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextSubID++
	id := m.nextSubID
	ch := make(chan struct{}, 1)
	set, ok := m.subs[afterSequence]
	if !ok {
		set = make(map[int64]chan struct{})
		m.subs[afterSequence] = set
	}
	set[id] = ch
	return &memSubscription{m: m, after: afterSequence, id: id, ch: ch}
}

func (m *MemoryStore) notifyLocked() {
	// 唤醒信号通道容量为 1：通知合并为“至少一次”，消费者始终以 ReadAll 为准。
	for _, set := range m.subs {
		for _, ch := range set {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
}

func (m *MemoryStore) unsubscribe(after, id int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if set, ok := m.subs[after]; ok {
		delete(set, id)
		if len(set) == 0 {
			delete(m.subs, after)
		}
	}
}

// Close 关闭存储并清理订阅。
func (m *MemoryStore) Close(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	m.closed = true
	for after, set := range m.subs {
		for id := range set {
			delete(set, id)
		}
		delete(m.subs, after)
	}
	return nil
}

type memSubscription struct {
	m     *MemoryStore
	after int64
	id    int64
	ch    chan struct{}
	once  sync.Once
}

func (s *memSubscription) C() <-chan struct{} { return s.ch }

func (s *memSubscription) Close() {
	s.once.Do(func() { s.m.unsubscribe(s.after, s.id) })
}

func cloneEvents(in []RecordedEvent) []RecordedEvent {
	out := make([]RecordedEvent, len(in))
	copy(out, in)
	for i := range out {
		out[i].Data = copyBytes(in[i].Data)
		if len(in[i].Metadata) > 0 {
			out[i].Metadata = copyBytes(in[i].Metadata)
		}
	}
	return out
}

func copyBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}

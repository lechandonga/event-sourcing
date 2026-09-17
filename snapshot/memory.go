package snapshot

import (
	"context"
	"sync"
)

// MemoryStore 是进程内快照存储。
type MemoryStore struct {
	mu     sync.Mutex
	snaps  map[string]*Snapshot
	closed bool
}

// NewMemoryStore 创建内存快照存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{snaps: make(map[string]*Snapshot)}
}

func snapKey(streamType, streamID string) string { return streamType + "|" + streamID }

// Save 保存快照（深拷贝，避免调用方之后修改）。快照必须已 Seal。
func (s *MemoryStore) Save(_ context.Context, snap *Snapshot) error {
	if err := snap.Verify(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSnapshotCorrupt
	}
	cp := *snap
	if len(snap.Payload) > 0 {
		cp.Payload = append([]byte(nil), snap.Payload...)
	}
	s.snaps[snapKey(snap.StreamType, snap.StreamID)] = &cp
	return nil
}

// Load 返回快照深拷贝并再次校验完整性。
func (s *MemoryStore) Load(_ context.Context, streamType, streamID string) (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, ok := s.snaps[snapKey(streamType, streamID)]
	if !ok {
		return nil, ErrSnapshotNotFound
	}
	cp := *snap
	if len(snap.Payload) > 0 {
		cp.Payload = append([]byte(nil), snap.Payload...)
	}
	if err := cp.Verify(); err != nil {
		return nil, err
	}
	return &cp, nil
}

// Count 返回当前保存的快照数量（测试/诊断用）。
func (s *MemoryStore) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.snaps)
}

// Delete 删除快照；不存在不算错误。
func (s *MemoryStore) Delete(_ context.Context, streamType, streamID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.snaps, snapKey(streamType, streamID))
	return nil
}

// Close 清空并标记关闭。
func (s *MemoryStore) Close(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.snaps = make(map[string]*Snapshot)
	return nil
}

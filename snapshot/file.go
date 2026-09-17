package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/lechandonga/event-sourcing/internal/safeio"
)

// FileStore 把每个聚合快照以独立 JSON 文件保存在本地目录，写入是原子的。
type FileStore struct {
	dir string
}

// NewFileStore 创建基于目录 dir 的快照存储（目录按需创建）。
func NewFileStore(dir string) *FileStore { return &FileStore{dir: dir} }

func (s *FileStore) pathOf(streamType, streamID string) string {
	return filepath.Join(s.dir, streamType, streamID+".snapshot.json")
}

// Save 原子写快照文件。
func (s *FileStore) Save(_ context.Context, snap *Snapshot) error {
	if err := snap.Verify(); err != nil {
		return err
	}
	// 紧凑编码：持久化字节必须与 Seal 时参与校验和计算的结构一一对应，
	// 不使用缩进/重排，避免“保存的格式”和“校验的内容”出现非语义差异。
	raw, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("snapshot: marshal: %w", err)
	}
	if err := safeio.WriteFile(s.pathOf(snap.StreamType, snap.StreamID), raw); err != nil {
		return fmt.Errorf("snapshot: save: %w", err)
	}
	return nil
}

// Load 读取快照；JSON/校验和损坏一律包装为 ErrSnapshotCorrupt，
// 使调用方能够统一走“删除损坏快照 + 从头重放”的恢复路径。
func (s *FileStore) Load(_ context.Context, streamType, streamID string) (*Snapshot, error) {
	raw, err := readFile(s.pathOf(streamType, streamID))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrSnapshotNotFound
		}
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return nil, fmt.Errorf("%w: unmarshal: %v", ErrSnapshotCorrupt, err)
	}
	if snap.StreamType != streamType || snap.StreamID != streamID {
		return nil, fmt.Errorf("%w: snapshot identity mismatch (file=%s/%s want=%s/%s)",
			ErrSnapshotCorrupt, snap.StreamType, snap.StreamID, streamType, streamID)
	}
	if err := snap.Verify(); err != nil {
		return nil, err
	}
	return &snap, nil
}

// Delete 删除快照文件；不存在不算错误。
func (s *FileStore) Delete(_ context.Context, streamType, streamID string) error {
	err := removeFile(s.pathOf(streamType, streamID))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// Close 对纯文件存储无资源需要释放。
func (s *FileStore) Close(_ context.Context) error { return nil }

// DirForTest 返回存储目录（仅供测试定位文件）。
func (s *FileStore) DirForTest() string { return s.dir }

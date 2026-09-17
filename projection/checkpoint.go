// Package projection 在事件日志之上维护读模型：
// 检查点续传、幂等有序重放、可重试的失败记录，以及不阻塞写入的在线重建。
package projection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/lechandonga/event-sourcing/internal/safeio"
)

// Checkpoint 记录某个投影“已确认处理到”的位置。
//
// Sequence 是已成功应用到读模型的最后一个事件的全局序号（0 表示尚未处理任何事件）。
// 检查点只有在事件成功 Apply 之后才会前移（at-least-once），
// 因此重复投递最多导致重复尝试，而投影侧以“序号 > 检查点”为闸门，天然幂等。
type Checkpoint struct {
	Projection string `json:"projection"`
	Sequence   int64  `json:"sequence"`
	UpdatedAt  string `json:"updatedAt,omitempty"`
}

// CheckpointStore 持久化检查点。
type CheckpointStore interface {
	Load(ctx context.Context, projection string) (Checkpoint, error)
	// Save 单调推进检查点；回退将被拒绝（ErrCheckpointRegression），
	// 防止乱序/并发写入意外覆盖进度。
	Save(ctx context.Context, cp Checkpoint) error
	// Reset 把检查点显式归零。只有“投影重建”这一受信流程可以调用：
	// 它语义上是一次有意的重来，而不是进度覆盖。
	Reset(ctx context.Context, projection string) error
}

// MemoryCheckpointStore 是进程内检查点存储。
type MemoryCheckpointStore struct {
	mu    sync.Mutex
	state map[string]int64
}

// NewMemoryCheckpointStore 创建内存检查点存储。
func NewMemoryCheckpointStore() *MemoryCheckpointStore {
	return &MemoryCheckpointStore{state: make(map[string]int64)}
}

// Load 读取检查点；不存在返回零值检查点（Sequence=0），不视为错误。
func (m *MemoryCheckpointStore) Load(_ context.Context, projection string) (Checkpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Checkpoint{Projection: projection, Sequence: m.state[projection]}, nil
}

// Save 单调保护：检查点不允许回退（防止乱序写入覆盖进度）。
func (m *MemoryCheckpointStore) Save(_ context.Context, cp Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur := m.state[cp.Projection]; cp.Sequence < cur {
		return fmt.Errorf("%w: checkpoint regression %d -> %d",
			ErrCheckpointRegression, cur, cp.Sequence)
	}
	m.state[cp.Projection] = cp.Sequence
	return nil
}

// Reset 显式归零检查点（重建用）。
func (m *MemoryCheckpointStore) Reset(_ context.Context, projection string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state[projection] = 0
	return nil
}

// ErrCheckpointRegression 表示检查点被尝试回退。
var ErrCheckpointRegression = errors.New("projection: checkpoint regression")

// FileCheckpointStore 把每个投影的检查点原子写入本地 JSON 文件。
type FileCheckpointStore struct{ dir string }

// NewFileCheckpointStore 创建目录型检查点存储。
func NewFileCheckpointStore(dir string) *FileCheckpointStore { return &FileCheckpointStore{dir: dir} }

func (f *FileCheckpointStore) pathOf(name string) string {
	return filepath.Join(f.dir, sanitizeName(name)+".checkpoint.json")
}

// Load 读取检查点文件；文件缺失视为 Sequence=0。
func (f *FileCheckpointStore) Load(_ context.Context, projection string) (Checkpoint, error) {
	raw, err := os.ReadFile(f.pathOf(projection))
	if errors.Is(err, os.ErrNotExist) {
		return Checkpoint{Projection: projection}, nil
	}
	if err != nil {
		return Checkpoint{}, fmt.Errorf("projection: read checkpoint: %w", err)
	}
	var cp Checkpoint
	if err := json.Unmarshal(raw, &cp); err != nil {
		return Checkpoint{}, fmt.Errorf("projection: corrupt checkpoint for %q: %w", projection, err)
	}
	if cp.Projection != projection {
		return Checkpoint{}, fmt.Errorf("projection: corrupt checkpoint for %q: identity=%q",
			projection, cp.Projection)
	}
	return cp, nil
}

// Reset 显式归零检查点（重建用），同样原子落盘；有意绕过单调保护。
func (f *FileCheckpointStore) Reset(ctx context.Context, projection string) error {
	return f.write(ctx, Checkpoint{Projection: projection, Sequence: 0})
}

// Save 原子覆盖检查点文件；同样做单调保护，回退被拒绝。
func (f *FileCheckpointStore) Save(ctx context.Context, cp Checkpoint) error {
	if cur, err := f.Load(ctx, cp.Projection); err == nil && cp.Sequence < cur.Sequence {
		return fmt.Errorf("%w: checkpoint regression %d -> %d",
			ErrCheckpointRegression, cur.Sequence, cp.Sequence)
	}
	return f.write(ctx, cp)
}

func (f *FileCheckpointStore) write(_ context.Context, cp Checkpoint) error {
	raw, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("projection: marshal checkpoint: %w", err)
	}
	if err := safeio.WriteFile(f.pathOf(cp.Projection), raw); err != nil {
		return fmt.Errorf("projection: save checkpoint: %w", err)
	}
	return nil
}

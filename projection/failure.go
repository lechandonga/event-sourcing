package projection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lechandonga/event-sourcing/internal/safeio"
)

// Failure 描述一次投影处理失败。检查点停在 Sequence-1，
// 该记录让运维能够定位“毒丸事件”并在修复后重试，期间不会跳过该事件。
type Failure struct {
	Projection      string `json:"projection"`
	Sequence        int64  `json:"sequence"`
	Stream          string `json:"stream"`
	StreamVersion   int64  `json:"streamVersion"`
	EventType       string `json:"eventType"`
	EventID         string `json:"eventId"`
	Message         string `json:"message"`
	FirstOccurredAt string `json:"firstOccurredAt"`
	LastOccurredAt  string `json:"lastOccurredAt"`
	Attempts        int    `json:"attempts"`
}

// FailureStore 保存当前阻塞投影的失败记录（每个投影至多一条）。
type FailureStore interface {
	Load(ctx context.Context, projection string) (*Failure, error)
	Save(ctx context.Context, f *Failure) error
	Clear(ctx context.Context, projection string) error
}

// MemoryFailureStore 是进程内失败记录存储。
type MemoryFailureStore struct {
	mu      sync.Mutex
	records map[string]*Failure
}

// NewMemoryFailureStore 创建内存失败记录存储。
func NewMemoryFailureStore() *MemoryFailureStore {
	return &MemoryFailureStore{records: make(map[string]*Failure)}
}

// Load 读取失败记录；不存在返回 (nil, nil)。
func (m *MemoryFailureStore) Load(_ context.Context, projection string) (*Failure, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f := m.records[projection]
	if f == nil {
		return nil, nil
	}
	cp := *f
	return &cp, nil
}

// Save 保存失败；同一投影同一事件重试时累加 Attempts 并更新信息。
func (m *MemoryFailureStore) Save(_ context.Context, f *Failure) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing := m.records[f.Projection]; existing != nil && existing.Sequence == f.Sequence {
		existing.Attempts++
		existing.LastOccurredAt = f.LastOccurredAt
		existing.Message = f.Message
		return nil
	}
	cp := *f
	if cp.Attempts == 0 {
		cp.Attempts = 1
	}
	m.records[f.Projection] = &cp
	return nil
}

// Clear 清除失败记录。
func (m *MemoryFailureStore) Clear(_ context.Context, projection string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.records, projection)
	return nil
}

// FileFailureStore 用两个本地文件保存失败信息：
//   - <name>.failures.jsonl：只追加的历史审计文件，每次失败/重试留痕；
//   - <name>.failure.json：原子覆盖的“当前阻塞失败”，清除时写空对象。
//
// 历史文件永不改写、不删除，用于事后定位；恢复逻辑只读当前文件。
type FileFailureStore struct{ dir string }

// NewFileFailureStore 创建目录型失败记录存储。
func NewFileFailureStore(dir string) *FileFailureStore {
	return &FileFailureStore{dir: dir}
}

func (f *FileFailureStore) historyPath(name string) string {
	return filepath.Join(f.dir, sanitizeName(name)+".failures.jsonl")
}

func (f *FileFailureStore) currentPath(name string) string {
	return filepath.Join(f.dir, sanitizeName(name)+".failure.json")
}

// Load 读取当前失败；侧车缺失或已清空均视为无失败。
func (f *FileFailureStore) Load(ctx context.Context, projection string) (*Failure, error) {
	raw, err := os.ReadFile(f.currentPath(projection))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("projection: read failure: %w", err)
	}
	var rec Failure
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, fmt.Errorf("projection: corrupt failure record for %q: %w", projection, err)
	}
	if rec.Projection == "" {
		return nil, nil
	}
	return &rec, nil
}

// Save 追加历史审计行并原子更新当前失败。
func (f *FileFailureStore) Save(ctx context.Context, rec *Failure) error {
	if existing, _ := f.Load(ctx, rec.Projection); existing != nil && existing.Sequence == rec.Sequence {
		rec.Attempts = existing.Attempts + 1
		rec.FirstOccurredAt = existing.FirstOccurredAt
	} else {
		if rec.Attempts == 0 {
			rec.Attempts = 1
		}
		if rec.FirstOccurredAt == "" {
			rec.FirstOccurredAt = rec.LastOccurredAt
		}
	}
	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("projection: marshal failure: %w", err)
	}
	line = append(line, '\n')
	if err := os.MkdirAll(f.dir, 0o755); err != nil {
		return fmt.Errorf("projection: mkdir failures: %w", err)
	}
	hf, err := os.OpenFile(f.historyPath(rec.Projection), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("projection: open failure history: %w", err)
	}
	if _, err := hf.Write(line); err != nil {
		_ = hf.Close()
		return fmt.Errorf("projection: write failure history: %w", err)
	}
	if err := hf.Sync(); err != nil {
		_ = hf.Close()
		return fmt.Errorf("projection: sync failure history: %w", err)
	}
	if err := hf.Close(); err != nil {
		return err
	}
	cur, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	if err := safeio.WriteFile(f.currentPath(rec.Projection), cur); err != nil {
		return fmt.Errorf("projection: save current failure: %w", err)
	}
	return nil
}

// Clear 原子地把当前失败标记为空（历史审计行保留）。
func (f *FileFailureStore) Clear(_ context.Context, projection string) error {
	if err := safeio.WriteFile(f.currentPath(projection), []byte("{}\n")); err != nil {
		return fmt.Errorf("projection: clear failure: %w", err)
	}
	return nil
}

func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339Nano) }

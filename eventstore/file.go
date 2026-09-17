package eventstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// fileMagic 是事件日志文件头，用于识别文件格式与版本。
const fileMagic = "ES-EVENTS-V1\n"

// FileStore 是把事件持久化在本地单个文件中的 Store。
//
// 布局：一行文件头 + 若干 JSON Lines；每行是一次原子提交的事件批次（JSON 数组）。
// 追加采用“两阶段提交”：
//  1. 在 wmu 串行区内校验乐观并发并分配版本/全局序号（内存 prepare）；
//  2. 把整批编码为一行 fsync 落盘；
//  3. 发布内存视图并通知订阅者。
//
// 崩溃若发生在第 2 步行写完前：最后一行缺少换行符，启动时被识别为半写行并截断；
// 已带换行符的完整行一定对应一次成功提交。
type FileStore struct {
	mem *MemoryStore

	path string
	f    *os.File
	wmu  sync.Mutex
}

// OpenFileStore 打开（不存在则创建）本地事件日志文件。
func OpenFileStore(ctx context.Context, path string) (*FileStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("eventstore: mkdir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("eventstore: open log: %w", err)
	}
	fs := &FileStore{
		mem:  NewMemoryStore(),
		path: path,
		f:    f,
	}
	if err := fs.load(ctx); err != nil {
		_ = f.Close()
		return nil, err
	}
	return fs, nil
}

func (s *FileStore) load(_ context.Context) error {
	raw, err := io.ReadAll(s.f)
	if err != nil {
		return fmt.Errorf("eventstore: read log: %w", err)
	}
	if len(raw) == 0 {
		if _, err := s.f.WriteString(fileMagic); err != nil {
			return fmt.Errorf("eventstore: write header: %w", err)
		}
		return s.syncFile()
	}
	if !bytes.HasPrefix(raw, []byte(fileMagic)) {
		return fmt.Errorf("eventstore: bad magic header: %w", ErrCorrupt)
	}
	body := raw[len(fileMagic):]

	// 切分行。最后一个不带 '\n' 的行是崩溃遗留的半写行：原样丢弃并截断文件。
	lines := splitCompleteLines(body)
	completeBytes := 0
	wantSeq := int64(1)
	streamVer := make(map[string]int64)
	streamType := make(map[string]string)
	for _, line := range lines {
		var batch []RecordedEvent
		if jerr := json.Unmarshal(bytes.TrimSpace(line), &batch); jerr != nil {
			return fmt.Errorf("eventstore: decode line: %w (%w)", jerr, ErrCorrupt)
		}
		if len(batch) == 0 {
			return fmt.Errorf("eventstore: empty batch line: %w", ErrCorrupt)
		}
		for _, e := range batch {
			if e.GlobalSequence != wantSeq {
				return fmt.Errorf("eventstore: global sequence gap: want %d got %d (%w)",
					wantSeq, e.GlobalSequence, ErrCorrupt)
			}
			wantSeq++
			ver := streamVer[e.Stream] + 1
			if e.Version != ver {
				return fmt.Errorf("eventstore: stream %s version gap: want %d got %d (%w)",
					e.Stream, ver, e.Version, ErrCorrupt)
			}
			streamVer[e.Stream] = ver
			if t, ok := streamType[e.Stream]; ok && t != e.StreamType {
				return fmt.Errorf("eventstore: stream %s aggregate type changed: %w",
					e.Stream, ErrCorrupt)
			}
			streamType[e.Stream] = e.StreamType
		}
		completeBytes += len(line)
		// 历史数据是权威事实；逐事件连续性已在上面验证，导入内存视图。
		if err := s.mem.ImportBatch(StreamID{
			Type: batch[0].StreamType,
			ID:   streamIDFromName(batch[0].Stream),
		}, batch); err != nil {
			return err
		}
	}

	// 截断半写行（若存在）。completeBytes 仅统计行体，文件偏移需加文件头。
	safeLen := int64(len(fileMagic)) + int64(completeBytes)
	info, err := s.f.Stat()
	if err != nil {
		return fmt.Errorf("eventstore: stat log: %w", err)
	}
	if info.Size() != safeLen {
		if info.Size() < safeLen {
			return fmt.Errorf("eventstore: safe length beyond file size: %w", ErrCorrupt)
		}
		if err := s.f.Truncate(safeLen); err != nil {
			return fmt.Errorf("eventstore: truncate partial write: %w (%w)", err, ErrCorrupt)
		}
	}
	if _, err := s.f.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("eventstore: seek end: %w", err)
	}
	return s.syncFile()
}

// splitCompleteLines 按 '\n' 切分，仅返回以 '\n' 结尾的完整行（保留换行符）。
// 末尾不含换行符的残余视为半写行，被忽略。
func splitCompleteLines(body []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, b := range body {
		if b == '\n' {
			out = append(out, body[start:i+1])
			start = i + 1
		}
	}
	return out
}

func (s *FileStore) syncFile() error {
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("eventstore: fsync log: %w", err)
	}
	// fsync 目录，保证新建文件与截断元数据落盘。
	if dir, err := os.Open(filepath.Dir(s.path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

// Append 走 prepare → 落盘 → 复检提交 的两阶段。
//
// wmu 串行化所有两阶段窗口，使 prepare 与 commit 之间不会穿插本存储的其他写入；
// 复检是第二道防线。若复检失败（例如将来出现绕过 wmu 的写路径），会把刚刚写入
// 的那一行从日志截断，保证磁盘与内存都不留下该批事件，返回冲突错误。
func (s *FileStore) Append(ctx context.Context, stream StreamID, expected ExpectedVersion, events []UncommittedEvent) ([]RecordedEvent, error) {
	s.wmu.Lock()
	defer s.wmu.Unlock()

	prepared, err := s.mem.prepareForDisk(stream, expected, events)
	if err != nil {
		return nil, err
	}
	line, err := json.Marshal(prepared)
	if err != nil {
		return nil, fmt.Errorf("eventstore: encode batch: %w", err)
	}
	line = append(line, '\n')
	offset, err := s.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("eventstore: tell log: %w", err)
	}
	if _, err := s.f.Write(line); err != nil {
		return nil, fmt.Errorf("eventstore: write log: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		return nil, fmt.Errorf("eventstore: fsync log: %w", err)
	}
	committed, cerr := s.mem.commitForDisk(stream, prepared)
	if cerr != nil {
		// 复检冲突：回滚磁盘行，截断到写入前偏移并 fsync。
		if terr := s.f.Truncate(offset); terr != nil {
			return nil, fmt.Errorf("eventstore: commit conflict %v AND rollback failed: %w", cerr, terr)
		}
		if _, terr := s.f.Seek(0, io.SeekEnd); terr != nil {
			return nil, terr
		}
		_ = s.f.Sync()
		return nil, cerr
	}
	return committed, nil
}

// ReadStream 委托给内存视图。
func (s *FileStore) ReadStream(ctx context.Context, stream StreamID, fromVersion, toVersion int64) ([]RecordedEvent, error) {
	return s.mem.ReadStream(ctx, stream, fromVersion, toVersion)
}

// StreamVersion 委托给内存视图。
func (s *FileStore) StreamVersion(ctx context.Context, stream StreamID) (int64, error) {
	return s.mem.StreamVersion(ctx, stream)
}

// ReadAll 委托给内存视图。
func (s *FileStore) ReadAll(ctx context.Context, afterSequence, upToSequence int64) ([]RecordedEvent, error) {
	return s.mem.ReadAll(ctx, afterSequence, upToSequence)
}

// LastSequence 委托给内存视图。
func (s *FileStore) LastSequence(ctx context.Context) (int64, error) {
	return s.mem.LastSequence(ctx)
}

// Subscribe 委托给内存视图。
func (s *FileStore) Subscribe(afterSequence int64) Subscription {
	return s.mem.Subscribe(afterSequence)
}

// Close 先同步再关闭文件。
func (s *FileStore) Close(_ context.Context) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if err := s.f.Sync(); err != nil {
		_ = s.f.Close()
		return err
	}
	return s.f.Close()
}

// streamIDFromName 从规范流名 "<type>-<id>" 还原 ID 部分。
// 类型始终以事件自带 StreamType 为准，这里仅用于重建 StreamID。
func streamIDFromName(name string) string {
	for i := 0; i < len(name); i++ {
		if name[i] == '-' {
			return name[i+1:]
		}
	}
	return name
}

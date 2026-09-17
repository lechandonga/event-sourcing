// Package safeio 提供“写临时文件 + fsync + 原子 rename”的本地原子写工具。
package safeio

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// WriteFile 原子地把 data 写入 path：同目录临时文件 -> fsync -> rename -> fsync 目录。
// 读到该文件的进程要么看到旧内容，要么看到完整新内容，绝不会看到截断的半成品。
func WriteFile(path string, data []byte) (err error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("safeio: mkdir %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".tmp-"+filepath.Base(path)+"-*")
	if err != nil {
		return fmt.Errorf("safeio: create temp: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()
	if _, err = io.Copy(tmp, bytesReader(data)); err != nil {
		return fmt.Errorf("safeio: write: %w", err)
	}
	if err = tmp.Sync(); err != nil {
		return fmt.Errorf("safeio: fsync: %w", err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("safeio: close: %w", err)
	}
	if err = os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("safeio: rename: %w", err)
	}
	// fsync 目录保证 rename 元数据持久化。
	if d, derr := os.Open(dir); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

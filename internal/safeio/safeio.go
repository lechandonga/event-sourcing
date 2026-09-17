// Package safeio provides small local-filesystem helpers used by the durable
// event store, snapshot store and checkpoint store:
//
//   - WriteFileAtomic: write a file via a sibling temp file + fsync + atomic
//     rename so that readers never observe a torn file.
//   - SafeName: map an arbitrary logical key (aggregate id, projection name)
//     to a stable, filesystem-safe file name.
//
// Everything is local storage only; no external service is involved.
package safeio

import (
	"fmt"
	"os"
	"path/filepath"
)

// WriteFileAtomic writes data to name atomically. On a crash the target file
// either keeps its previous complete content or becomes the new complete
// content; a partially written file is never visible at name.
func WriteFileAtomic(name string, data []byte, perm os.FileMode) (err error) {
	dir := filepath.Dir(name)
	if err = os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("safeio: mkdir %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("safeio: create temp in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmpName)
		}
	}()
	if _, err = tmp.Write(data); err != nil {
		return fmt.Errorf("safeio: write %s: %w", name, err)
	}
	if err = tmp.Sync(); err != nil {
		return fmt.Errorf("safeio: fsync %s: %w", name, err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("safeio: close %s: %w", name, err)
	}
	if err = os.Chmod(tmpName, perm); err != nil {
		return fmt.Errorf("safeio: chmod %s: %w", name, err)
	}
	if err = os.Rename(tmpName, name); err != nil {
		return fmt.Errorf("safeio: rename %s: %w", name, err)
	}
	// Best-effort fsync of the directory so the rename itself is durable.
	if d, derr := os.Open(dir); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// SafeName maps an arbitrary key to a deterministic filename-safe token.
func SafeName(key string) string {
	if key == "" {
		return "_"
	}
	out := make([]byte, 0, len(key))
	for i := 0; i < len(key); i++ {
		c := key[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '-', c == '_':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}

// EnsureDir creates dir (and parents) if missing.
func EnsureDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("safeio: mkdir %s: %w", dir, err)
	}
	return nil
}

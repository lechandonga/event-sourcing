package repo_test

import (
	"encoding/json"
	"os"
)

func jsonMarshal(v any) ([]byte, error)       { return json.Marshal(v) }
func readFileBytes(p string) ([]byte, error)  { return os.ReadFile(p) }
func writeFileBytes(p string, b []byte) error { return os.WriteFile(p, b, 0o644) }

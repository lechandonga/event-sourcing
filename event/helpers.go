package event

import (
	"bytes"
	"encoding/json"
)

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

// numberToInt 把 json.Number / float64 / int 等安全地转成 int。
func numberToInt(v any) (int, error) {
	switch n := v.(type) {
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, err
		}
		return int(i), nil
	case float64:
		return int(n), nil
	case int:
		return n, nil
	case int64:
		return int(n), nil
	default:
		return 0, ErrBadSchemaVersion
	}
}

// GetInt64 从升级中的事件 map 读取整数字段；缺失或为 null 时返回 def。
// 支持 json.Number（UseNumber 解码结果）与 float64。
func GetInt64(m map[string]any, key string, def int64) int64 {
	v, ok := m[key]
	if !ok || v == nil {
		return def
	}
	switch n := v.(type) {
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i
		}
		if f, err := n.Float64(); err == nil {
			return int64(f)
		}
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	}
	return def
}

// GetString 读取字符串字段；缺失/类型不符返回 def。
func GetString(m map[string]any, key, def string) string {
	v, ok := m[key]
	if !ok {
		return def
	}
	if s, ok := v.(string); ok {
		return s
	}
	return def
}

// RenameField 把旧字段名迁移到新字段名（仅当新字段缺失时），并删除旧字段。
func RenameField(m map[string]any, oldKey, newKey string) {
	if _, exists := m[newKey]; !exists {
		if v, ok := m[oldKey]; ok {
			m[newKey] = v
		}
	}
	delete(m, oldKey)
}

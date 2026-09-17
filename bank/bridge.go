package bank

import (
	"github.com/lechandonga/event-sourcing/event"
)

// 事件包辅助函数的薄封装，避免在域文件里分散 import。
func eventpkgGetInt64(m map[string]any, key string, def int64) int64 {
	return event.GetInt64(m, key, def)
}

func toEventUpcasters(in map[int]func(map[string]any)) map[int]event.Upcaster {
	out := make(map[int]event.Upcaster, len(in))
	for k, v := range in {
		out[k] = event.Upcaster(v)
	}
	return out
}

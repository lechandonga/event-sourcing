// Package event 提供事件类型注册与“按读取时升级”（upcast）的结构演进机制。
//
// 核心原则：
//   - 事件日志中已经提交的原始 JSON 永不改写；
//   - 每次读取历史事件时，按 schemaVersion 逐级执行注册的 Upcaster，
//     先升级到当前结构，再解码成当前 Go 类型参与重放；
//   - 升级只发生在读取副本上，对存储零影响。
//
// 兼容规则（稳定语义）：
//   - 新增字段：历史事件缺该字段时，由 upcaster 填入确定的默认值；
//   - 删除字段：旧事件多出的未知字段解码时自然忽略，不报错；
//   - 字段语义变更：由 upcaster 做确定性转换（如单位换算），不依赖任何外部状态；
//   - 升级必须是纯函数：相同输入永远得到相同输出。
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
)

// CurrentVersion 是“当前结构”的约定版本号：事件负载中省略 schemaVersion 时按当前处理。
const CurrentVersion = 0

var (
	// ErrUnknownEventType 遇到未注册的事件类型。
	ErrUnknownEventType = errors.New("event: unknown event type")
	// ErrBadSchemaVersion 事件携带了无法处理的 schemaVersion。
	ErrBadSchemaVersion = errors.New("event: bad schema version")
)

// Versioned 由当前事件结构实现，报告其当前 schema 版本。
type Versioned interface {
	// EventSchemaVersion 返回该结构当前的 schema 版本。
	EventSchemaVersion() int
}

// Upcaster 把事件负载从 fromVersion 升级到 fromVersion+1。
// 实现必须是确定性的纯函数，只修改入参 map，不读外部状态。
type Upcaster func(data map[string]any)

// Registry 保存事件类型与升级链。并发安全。
type Registry struct {
	mu        sync.RWMutex
	factories map[string]func() any
	versions  map[string]int
	upcasters map[string]map[int]Upcaster // type -> fromVersion -> upgrader
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[string]func() any),
		versions:  make(map[string]int),
		upcasters: make(map[string]map[int]Upcaster),
	}
}

// Register 注册一个当前事件类型，并可声明若干逐级升级器。
//
// prototype 是当前结构的零值指针，例如 (*AccountOpened)(nil)。
// upcasters 的 key 是“源版本”：1 表示把 v1 升级为 v2。
// 当前版本由 prototype 实现的 Versioned 接口推断；未实现则视为版本 1
// （此时不允许注册任何升级器）。
func (r *Registry) Register(prototype any, upcasters map[int]Upcaster) error {
	t, err := typeName(prototype)
	if err != nil {
		return err
	}
	pt := reflect.TypeOf(prototype)
	if pt.Kind() != reflect.Ptr {
		return fmt.Errorf("event: prototype %T must be a pointer", prototype)
	}
	elem := pt.Elem()
	// 原型允许是 nil 指针（(*MyEvent)(nil)）；用反射构造零值实例来安全调用
	// 值接收者方法，避免对用户传入的 nil 做接口解引用。
	zero := reflect.New(elem).Interface()
	current := 1
	if v, ok := zero.(Versioned); ok {
		current = v.EventSchemaVersion()
		if current < 1 {
			return fmt.Errorf("event: %T current version must be >= 1", prototype)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.factories[t]; exists {
		return fmt.Errorf("event: type %q registered twice", t)
	}
	r.factories[t] = func() any { return reflect.New(elem).Interface() }
	r.versions[t] = current
	if len(upcasters) > 0 {
		chain := make(map[int]Upcaster, len(upcasters))
		for from, u := range upcasters {
			if from < 1 || from >= current {
				return fmt.Errorf("event: %q upcaster source version %d out of range [1,%d)",
					t, from, current)
			}
			if u == nil {
				return fmt.Errorf("event: %q nil upcaster at version %d", t, from)
			}
			chain[from] = u
		}
		r.upcasters[t] = chain
	}
	return nil
}

// MustRegister 注册失败时 panic（用于包级初始化）。
func (r *Registry) MustRegister(prototype any, upcasters map[int]Upcaster) {
	if err := r.Register(prototype, upcasters); err != nil {
		panic(err)
	}
}

// Decode 取一段历史事件负载，先逐级升级到当前结构，再解码为当前 Go 事件。
//
// raw 中可选字段 "schemaVersion"（数字）标识结构版本；缺失或 0 视为当前版本。
// 升级过程中只操作 raw 的副本，函数返回后调用方持有的 raw 不发生任何变化。
func (r *Registry) Decode(eventType string, raw json.RawMessage) (any, int, error) {
	r.mu.RLock()
	factory, ok := r.factories[eventType]
	r.mu.RUnlock()
	if !ok {
		return nil, 0, fmt.Errorf("%w: %s", ErrUnknownEventType, eventType)
	}

	// UseNumber 保证大整数不被 float64 污染（upcaster 做单位换算时必须精确保留）。
	dec := json.NewDecoder(bytesReader(raw))
	dec.UseNumber()
	var generic map[string]any
	if err := dec.Decode(&generic); err != nil {
		return nil, 0, fmt.Errorf("event: decode %s payload: %w", eventType, err)
	}
	if generic == nil {
		return nil, 0, fmt.Errorf("event: %s payload must be a JSON object", eventType)
	}

	from := CurrentVersion
	if v, present := generic["schemaVersion"]; present {
		n, err := numberToInt(v)
		if err != nil {
			return nil, 0, fmt.Errorf("event: %s schemaVersion: %w", eventType, err)
		}
		from = n
	}

	r.mu.RLock()
	current := r.versions[eventType]
	chain := r.upcasters[eventType]
	r.mu.RUnlock()

	if from == CurrentVersion {
		from = current // 省略版本号一律视为“当前写入时的最新结构”
	}
	if from < 1 || from > current {
		return nil, 0, fmt.Errorf("%w: %s has schemaVersion %d, registry current is %d",
			ErrBadSchemaVersion, eventType, from, current)
	}
	for v := from; v < current; v++ {
		u := chain[v]
		if u == nil {
			return nil, 0, fmt.Errorf("%w: %s missing upcaster from v%d",
				ErrBadSchemaVersion, eventType, v)
		}
		u(generic)
		generic["schemaVersion"] = current // 最终标记；中间版本由循环顺序保证
	}
	// 统一盖成当前版本，避免上面循环只在有升级时才写入。
	generic["schemaVersion"] = current

	upgraded, err := json.Marshal(generic)
	if err != nil {
		return nil, 0, fmt.Errorf("event: remarshal %s: %w", eventType, err)
	}
	instance := factory()
	if err := json.Unmarshal(upgraded, instance); err != nil {
		return nil, 0, fmt.Errorf("event: decode upgraded %s: %w", eventType, err)
	}
	return instance, from, nil
}

// CurrentVersionOf 返回某事件类型在注册表中的当前 schema 版本。
func (r *Registry) CurrentVersionOf(eventType string) (int, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.versions[eventType]
	return v, ok
}

// RegisteredTypes 返回已注册类型名（排好序，便于诊断/文档生成）。
func (r *Registry) RegisteredTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.factories))
	for t := range r.factories {
		names = append(names, t)
	}
	sort.Strings(names)
	return names
}

// Encode 把当前事件结构编码为原始负载，并写入当前 schemaVersion。
func Encode(v Versioned) (json.RawMessage, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, err
	}
	generic["schemaVersion"] = v.EventSchemaVersion()
	return json.Marshal(generic)
}

func typeName(prototype any) (string, error) {
	// 允许传 T 的 nil 指针（如 (*MyEvent)(nil)）作为原型：先把元素类型零值化，
	// 再用它安全调用值接收者的 EventType()；没有该方法时回退到反射类型名。
	t := reflect.TypeOf(prototype)
	if t.Kind() != reflect.Ptr {
		return "", fmt.Errorf("event: prototype %T must be a pointer", prototype)
	}
	zero := reflect.New(t.Elem()).Interface()
	if named, ok := zero.(interface{ EventType() string }); ok {
		if named.EventType() == "" {
			return "", fmt.Errorf("event: %T returns empty EventType", prototype)
		}
		return named.EventType(), nil
	}
	name := t.Elem().Name()
	if name == "" {
		return "", fmt.Errorf("event: %s is not a named type and has no EventType()", t.String())
	}
	return name, nil
}

// TypeName 返回 prototype 的注册类型名（导出给测试/工具使用）。
func TypeName(prototype any) (string, error) { return typeName(prototype) }

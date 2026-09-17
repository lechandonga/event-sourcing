// Package snapshot 提供聚合快照的本地持久化与完整性校验。
//
// 快照携带“足以定位事件流位置”的信息：聚合类型、ID、快照所基于的最后一个
// 事件的流内版本（Version）与全局序号（GlobalSequence）。加载方必须从
// Version+1 继续重放；当快照缺失、过期、超前或校验损坏时，调用方一律可以
// 安全回退到从版本 0 开始的完整重放，不会跳过任何事件。
package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
)

// Snapshot 是快照的信封；Payload 是调用方自定义的聚合状态序列化结果。
type Snapshot struct {
	// StreamType / StreamID 定位所属聚合。
	StreamType string `json:"streamType"`
	StreamID   string `json:"streamId"`
	// Version 是快照包含的最后一个事件在流内的版本（从 1 起）；
	// 0 表示“空聚合快照”（合法，仍需从头重放，但允许保存初始状态）。
	Version int64 `json:"version"`
	// GlobalSequence 是该事件的全局序号，用于诊断与交叉校验。
	GlobalSequence int64 `json:"globalSequence"`
	// Payload 是聚合状态的不透明 JSON。
	Payload json.RawMessage `json:"payload"`
	// Checksum 是除 Checksum 字段外整个快照 JSON 的 CRC-32C（Castagnoli）校验和。
	Checksum uint32 `json:"checksum"`
	// CreatedAt 仅用于诊断。
	CreatedAt string `json:"createdAt,omitempty"`
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// checksumValue 计算除 checksum 字段外所有字段（含 payload 原文）的 CRC-32C。
// 用独立 shadow 结构序列化，保证“计算与存储格式解耦”，未来外层新增字段时
// 也不会因 omitempty 或字段顺序的细微差异导致误判。
func (s *Snapshot) checksumValue() (uint32, error) {
	type shadow struct {
		StreamType     string          `json:"streamType"`
		StreamID       string          `json:"streamId"`
		Version        int64           `json:"version"`
		GlobalSequence int64           `json:"globalSequence"`
		Payload        json.RawMessage `json:"payload"`
		CreatedAt      string          `json:"createdAt,omitempty"`
	}
	raw, err := json.Marshal(shadow{
		StreamType:     s.StreamType,
		StreamID:       s.StreamID,
		Version:        s.Version,
		GlobalSequence: s.GlobalSequence,
		Payload:        s.Payload,
		CreatedAt:      s.CreatedAt,
	})
	if err != nil {
		return 0, err
	}
	return crc32.Checksum(raw, castagnoli), nil
}

// Seal 计算并写入校验和。保存前必须调用。
func (s *Snapshot) Seal() error {
	csum, err := s.checksumValue()
	if err != nil {
		return err
	}
	s.Checksum = csum
	return nil
}

// Verify 校验校验和是否与内容一致。
func (s *Snapshot) Verify() error {
	want, err := s.checksumValue()
	if err != nil {
		return fmt.Errorf("snapshot: compute checksum: %w", err)
	}
	if want != s.Checksum {
		return fmt.Errorf("%w: checksum mismatch (stored %08x, computed %08x)",
			ErrSnapshotCorrupt, s.Checksum, want)
	}
	return nil
}

var (
	// ErrSnapshotNotFound 快照不存在（不是错误数据，调用方应从头重放）。
	ErrSnapshotNotFound = errors.New("snapshot: not found")
	// ErrSnapshotCorrupt 快照损坏：校验和不符 / JSON 无法解析 / 字段非法。
	ErrSnapshotCorrupt = errors.New("snapshot: corrupt")
)

// Store 是单聚合快照存储。键为 (streamType, streamID)。
type Store interface {
	// Save 原子覆盖保存快照（snap 必须已 Seal）。
	Save(ctx context.Context, snap *Snapshot) error
	// Load 读取并校验快照。不存在返回 ErrSnapshotNotFound；
	// 内容损坏返回包装了 ErrSnapshotCorrupt 的错误。
	Load(ctx context.Context, streamType, streamID string) (*Snapshot, error)
	// Delete 删除快照（损坏清理用）；不存在不算错误。
	Delete(ctx context.Context, streamType, streamID string) error
	// Close 释放资源。
	Close(ctx context.Context) error
}

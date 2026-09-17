package snapshot

import (
	"encoding/json"
	"hash/crc32"
)

type checksumBody struct {
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	Version       int             `json:"version"`
	SchemaVersion int             `json:"state_schema_version"`
	TakenAt       string          `json:"taken_at"`
	State         json.RawMessage `json:"state"`
}

// checksum computes CRC32-IEEE over the canonical JSON of every field except
// Checksum itself.
func checksum(s *Snapshot) uint32 {
	b, _ := json.Marshal(checksumBody{
		AggregateType: s.AggregateType,
		AggregateID:   s.AggregateID,
		Version:       s.Version,
		SchemaVersion: s.SchemaVersion,
		TakenAt:       s.TakenAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
		State:         s.State,
	})
	return crc32.ChecksumIEEE(b)
}

func verify(s *Snapshot) error {
	if checksum(s) != s.Checksum {
		return &CorruptionError{AggregateType: s.AggregateType, AggregateID: s.AggregateID,
			Reason: "checksum mismatch"}
	}
	return nil
}

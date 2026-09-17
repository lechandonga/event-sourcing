package eventstore

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"time"
)

// NewEventID generates a stable-ish unique event id: unix-nano time prefix
// plus 16 random hex chars. Ids are local only and need no coordination.
func NewEventID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// rand.Read only fails on a broken runtime; fall back to time.
		return "evt-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return "evt-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + hex.EncodeToString(b[:])
}

package eventstore

import (
	"crypto/rand"
	"encoding/hex"
)

// newEventID 生成一个 128 位随机事件 ID。
func newEventID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 在 Linux 上几乎不会失败；直接 panic 属于不可恢复的环境问题。
		panic("eventstore: cannot read random bytes: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

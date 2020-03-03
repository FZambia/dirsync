package hashutil

import (
	"crypto/sha256"
	"encoding/hex"
	"hash/adler32"
)

// WeakChecksum uses adler32 hash.
func WeakChecksum(data []byte) uint32 {
	weak := adler32.New()
	weak.Write(data)
	return weak.Sum32()
}

// StrongChecksum uses sha256 hash.
func StrongChecksum(data []byte) string {
	weak := sha256.New()
	weak.Write(data)
	return hex.EncodeToString(weak.Sum(nil))
}

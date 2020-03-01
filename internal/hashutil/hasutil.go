package hashutil

import (
	"crypto/sha256"
	"encoding/hex"
	"hash/adler32"
)

// WeakChecksum ...
func WeakChecksum(chunk []byte) uint32 {
	weak := adler32.New()
	weak.Write(chunk)
	return weak.Sum32()
}

// StrongChecksum ...
func StrongChecksum(chunk []byte) string {
	weak := sha256.New()
	weak.Write(chunk)
	return hex.EncodeToString(weak.Sum(nil))
}

package store

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

/* ==================================================================================
                            Utility functions
================================================================================== */

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

func dataKeyOf(rawKey []byte) []byte {
	key := fmt.Sprintf("%s%s", dbDatPrefix, hex.EncodeToString(rawKey))
	if log.IsTrace() {
		log.Trace("badger key", "dat", key)
	}
	return []byte(key)
}

func logKeyOf(idxKey uint64) []byte {
	key := fmt.Sprintf("%s%d", dbLogPrefix, idxKey)
	if log.IsTrace() {
		log.Trace("badger key", "log", key)
	}
	return []byte(key)
}

func u64KeyOf(rawKey []byte) []byte {
	key := fmt.Sprintf("%s%s", dbU64Prefix, hex.EncodeToString(rawKey))
	if log.IsDebug() {
		log.Trace("badger key", "u64", key)
	}
	return []byte(key)
}

func sstKeyOf(rawKey []byte) []byte {
	key := fmt.Sprintf("%s%s", dbSstPrefix, hex.EncodeToString(rawKey))
	if log.IsTrace() {
		log.Trace("badger key", "sst", key)
	}
	return []byte(key)
}

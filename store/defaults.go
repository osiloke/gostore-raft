package store

import (
	"errors"

	"github.com/hashicorp/go-hclog"
)

const (
	CMDREPLAY    = "replay"
	CMDSET       = "set"
	CMDMOD       = "mod"
	CMDDEL       = "del"
	BDGLOGPREFIX = "rft:"
	BDGSSTPREFIX = "sst:"
	BDGDATPREFIX = "dat:"
	REPLAYPREFIX = "rep:"
	BDGU64PREFIX = "u64:"
)

var (
	dbLogPrefix    = []byte(BDGLOGPREFIX) // Bucket names we perform transactions in
	dbDatPrefix    = []byte(BDGDATPREFIX)
	dbU64Prefix    = []byte(BDGU64PREFIX)
	dbSstPrefix    = []byte(BDGSSTPREFIX)
	dbReplayPrefix = []byte(REPLAYPREFIX)
	ErrKeyNotFound = errors.New("not found")
)

var (
	log = hclog.New(&hclog.LoggerOptions{Name: "gostore_raft"})
)

type IteratorRange struct{ from, to uint64 }

type RpcResponse struct {
	Error error
	Data  interface{}
}

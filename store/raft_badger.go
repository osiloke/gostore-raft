package store

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/dgraph-io/badger"
	"github.com/hashicorp/raft"
)

type raftBadger DefaultStore

// Apply applies a Raft log entry to the key-value store.
func (b *raftBadger) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch l.Type {
	case raft.LogCommand:
		var payload = command{}
		if err := json.Unmarshal(l.Data, &payload); err != nil {
			log.Error("error un-marshaling payload", "cause", err.Error())
			return nil
		}
		switch payload.Op {
		case CMDSET:
			_, err := b.gs.Save(payload.Key, payload.Store, payload.Value)
			return &RpcResponse{Error: err, Data: payload.Value}
		case CMDDEL:
			err := b.gs.Delete(payload.Key, payload.Store)
			return &RpcResponse{Error: err, Data: nil}
		default:
			log.Warn("Invalid Raft log command", "payload", payload.Op)
		}
	}
	log.Info("Raft log command", "type", raft.LogCommand)
	return nil
}

// Snapshot returns a snapshot of the key-value store.
func (b *raftBadger) Snapshot() (raft.FSMSnapshot, error) {
	// b.mu.Lock()
	// defer b.mu.Unlock()

	// // Clone the map.
	// o := make(map[string]string)
	// for k, v := range b.m {
	// 	o[k] = v
	// }
	// return &fsmSnapshot{store: o}, nil
	return &fsmSnapshot{}, nil
}

// Restore stores the key-value store to a previous state.
func (b *raftBadger) Restore(rc io.ReadCloser) error {
	// o := make(map[string]string)
	// if err := json.NewDecoder(rc).Decode(&o); err != nil {
	// 	return err
	// }

	// // Set the state from the snapshot, no lock required according to
	// // Hashicorp docs.
	// b.m = o
	return nil
}
func (s *raftBadger) Persist(_ raft.SnapshotSink) error {
	return nil
}

func (s *raftBadger) Release() {}

/* ==================================================================================
                            Raw access operations
================================================================================== */

func (b *raftBadger) GetRaw(k []byte) ([]byte, error) {
	txn := b.gs.GetStore().(*badger.DB).NewTransaction(false)
	defer txn.Discard()
	item, err := txn.Get(k)
	if item == nil {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}
	v, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	if err := txn.Commit(); err != nil {
		return nil, err
	}
	return append([]byte(nil), v...), nil
}

func (b *raftBadger) SetRaw(k []byte, v []byte) error {
	return b.gs.GetStore().(*badger.DB).Update(func(txn *badger.Txn) error {
		return txn.Set(k, v)
	})
}

func (b *raftBadger) DeleteRaw(key []byte) error {
	return b.gs.GetStore().(*badger.DB).Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// GetUint64 is like Get, but handles uint64 values
func (b *raftBadger) GetUint64(key []byte) (uint64, error) {
	val, err := b.GetRaw(u64KeyOf(key))
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}

// SetUint64 is like Set, but handles uint64 values
func (b *raftBadger) SetUint64(key []byte, val uint64) error {
	return b.SetRaw(u64KeyOf(key), uint64ToBytes(val))
}

/* ==================================================================================
                            Log operations
================================================================================== */

func (b *raftBadger) generateRanges(min, max uint64, batchSize int64) []IteratorRange {
	nSegments := int(math.Round(float64((max - min) / uint64(batchSize))))
	segments := []IteratorRange{}
	if (max - min) <= uint64(batchSize) {
		segments = append(segments, IteratorRange{from: min, to: max})
		return segments
	}
	for len(segments) < nSegments {
		nextMin := min + uint64(batchSize)
		segments = append(segments, IteratorRange{from: min, to: nextMin})
		min = nextMin + 1
	}
	segments = append(segments, IteratorRange{from: min, to: max})
	return segments
}

// FirstIndex returns the first known index from the Raft log.
func (b *raftBadger) FirstIndex() (uint64, error) {
	first := uint64(0)
	err := b.gs.GetStore().(*badger.DB).View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(dbLogPrefix)
		if it.ValidForPrefix(dbLogPrefix) {
			item := it.Item()
			k := string(item.Key()[len(dbLogPrefix):])
			idx, err := strconv.ParseUint(k, 10, 64)
			if err != nil {
				return err
			}
			first = idx
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return first, nil
}

// LastIndex returns the last known index from the Raft log.
func (b *raftBadger) LastIndex() (uint64, error) {
	last := uint64(0)
	if err := b.gs.GetStore().(*badger.DB).View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		// see https://github.com/dgraph-io/badger/issues/436
		// and https://github.com/dgraph-io/badger/issues/347
		seekKey := append(dbLogPrefix, 0xFF)
		it.Seek(seekKey)
		if it.ValidForPrefix(dbLogPrefix) {
			item := it.Item()
			k := string(item.Key()[len(dbLogPrefix):])
			idx, err := strconv.ParseUint(k, 10, 64)
			if err != nil {
				return err
			}
			last = idx
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return last, nil
}

// GetLog is used to retrieve a log from Badger at a given index.
func (b *raftBadger) GetLog(idx uint64, log *raft.Log) error {
	return b.gs.GetStore().(*badger.DB).View(func(txn *badger.Txn) error {
		item, _ := txn.Get(logKeyOf(idx))
		if item == nil {
			return raft.ErrLogNotFound
		}
		err := item.Value(func(val []byte) error {
			buf := bytes.NewBuffer(val)
			dec := gob.NewDecoder(buf)
			return dec.Decode(&log)
		})
		return err
	})
}

// StoreLogs is used to store a set of raft logs
func (b *raftBadger) StoreLogs(logs []*raft.Log) error {
	maxBatchSize := b.gs.GetStore().(*badger.DB).MaxBatchSize()
	min := uint64(0)
	max := uint64(len(logs))
	ranges := b.generateRanges(min, max, maxBatchSize)
	for _, r := range ranges {
		txn := b.gs.GetStore().(*badger.DB).NewTransaction(true)
		defer txn.Discard()
		for index := r.from; index < r.to; index++ {
			log := logs[index]
			var out bytes.Buffer
			enc := gob.NewEncoder(&out)
			enc.Encode(log)
			if err := txn.Set(logKeyOf(log.Index), out.Bytes()); err != nil {
				return err
			}
		}
		if err := txn.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// StoreLog is used to store a single raft log
func (b *raftBadger) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

// DeleteRange is used to delete logs within a given range inclusively.
func (b *raftBadger) DeleteRange(min, max uint64) error {
	maxBatchSize := b.gs.GetStore().(*badger.DB).MaxBatchSize()
	ranges := b.generateRanges(min, max, maxBatchSize)
	for _, r := range ranges {
		txn := b.gs.GetStore().(*badger.DB).NewTransaction(true)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer txn.Discard()

		it.Rewind()
		minKey := logKeyOf(r.from) // Get the key to start at
		for it.Seek(minKey); it.ValidForPrefix(dbLogPrefix); it.Next() {
			item := it.Item()
			k := string(item.Key()[len(dbLogPrefix):]) // Get the index as a string to convert to uint64
			idx, err := strconv.ParseUint(k, 10, 64)
			if err != nil {
				it.Close()
				return err
			}
			if idx > r.to { // Handle out-of-range index
				break
			}
			delKey := logKeyOf(idx) // Delete in-range index
			if err := txn.Delete(delKey); err != nil {
				it.Close()
				return err
			}
		}
		it.Close()
		if err := txn.Commit(); err != nil {
			return err
		}
	}
	return nil
}

/* ==================================================================================
                            Additional implementations
================================================================================== */

// Get a value in StableStore.
func (b *raftBadger) Get(k []byte) ([]byte, error) {
	log.Debug(fmt.Sprintf("get %s", string(k)))
	return b.GetRaw(sstKeyOf(k))
}

// Set a key/value in StableStore.
func (b *raftBadger) Set(k []byte, v []byte) error {
	return b.SetRaw(sstKeyOf(k), v)
}

func (b *raftBadger) Close() error {
	b.gs.Close()
	return nil
}

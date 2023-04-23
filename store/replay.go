package store

import (
	"encoding/json"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	badgerdb "github.com/dgraph-io/badger"
)

func (s *DefaultStore) replayStatus() (bool, []byte) {
	file, err := os.Open(filepath.Join(s.RaftDir, "raft.replay"))
	if err != nil {
		// Check if the error is related to file not found
		if os.IsNotExist(err) {
			s.logger.Info("Replay not required")
			return false, nil
		}
		panic(err)
	}
	defer file.Close()
	lastKey, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}
	return true, lastKey
}

func (s *DefaultStore) writeLastReplyedKey(key []byte) error {
	return ioutil.WriteFile(filepath.Join(s.RaftDir, "raft.replay"), key, fs.ModePerm)

}
func (s *DefaultStore) setReplay(value bool) error {
	buf := make([]byte, 1)
	buf[0] = 0
	if value {
		buf[0] = 1
	}
	//log replay status
	c := &command{
		Op:    "replay",
		Store: "status",
		Value: buf,
	}

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}
func (s *DefaultStore) replay() error {
	var err error
	replay, lastKey := s.replayStatus()
	if replay {
		logCounter := 0
		if err = s.setReplay(true); err == nil {
			s.raftReplaying = true
			s.logger.Info("Replaying logs from database")
			// open replay store
			db := s.gs.GetStore().(*badgerdb.DB)
			err = db.View(func(txn *badgerdb.Txn) error {
				keyWriteCounter := 0
				var c *command
				opts := badgerdb.DefaultIteratorOptions
				opts.PrefetchSize = 10
				opts.Reverse = true
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Seek(lastKey); it.Valid(); it.Next() {
					item := it.Item()
					k := item.Key()
					obj := make([][]byte, 2)
					err := item.Value(func(v []byte) error {
						obj[1] = append([]byte{}, v...)
						return nil
					})
					if err != nil {
						return err
					}
					obj[0] = make([]byte, len(k))
					copy(obj[0], k)
					unsplit := strings.Split(strings.TrimLeft(string(k), "t$"), "|")
					store := unsplit[0]
					key := unsplit[1]

					value := map[string]interface{}{}
					err = json.Unmarshal(obj[1], &value)

					if err == nil {
						c = &command{
							Op:    "set",
							Store: store,
							Key:   key,
							Value: value,
						}
					} else {
						c = &command{
							Op:    "set",
							Store: store,
							Key:   key,
							Value: string(obj[1]),
						}
					}
					b, err := json.Marshal(c)
					if err != nil {
						return err
					}

					f := s.raft.Apply(b, raftTimeout)
					if err := f.Error(); err != nil {
						return err
					}
					keyWriteCounter++
					logCounter++
					s.logger.Debug("Reapplying key", "store", store, "key", key) //, "value", hclog.Fmt("%s", string(obj[1])))
					if keyWriteCounter == 100 {
						s.logger.Info("writing last replayed key", "key", string(k))
						if err := s.writeLastReplyedKey(k); err != nil {
							return err
						}
						// s.raft.Snapshot()
					}
				}
				s.raftReplaying = false
				return s.setReplay(false)
			})
			if err == nil {
				return os.Remove(filepath.Join(s.RaftDir, "raft.replay"))
			}
		}
	}
	return err
}

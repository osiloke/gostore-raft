package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gosimple/slug"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/osiloke/gostore"
	"github.com/osiloke/gostore-contrib/badger"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type command struct {
	Op    string      `json:"op,omitempty"`
	Key   string      `json:"key,omitempty"`
	Store string      `json:"store,omitempty"`
	Value interface{} `json:"value,omitempty"`
}

// RaftStore represents a store that works with raft.
type RaftStore interface {
	Join(string, string) error
	Start() error
	Open(bool) error
	Close()
	GetConfiguration() raft.Configuration
	Bootstrap(nodes [][]string) error
	Leader() raft.ServerAddress
	IsLeader() bool
	Leave(peerId string) error
	Stats() map[string]string
}

// Store is an interface generated for "github.com/osiloke/gostore_raft/store".Store.
type Store interface {
	Delete(string, string) error
	Set(string, string, interface{}) error
	DataStore() gostore.ObjectStore
}

// DefaultStore is a simple key-value store, where all changes are made via Raft consensus.
type DefaultStore struct {
	RaftDir           string
	RaftBind          string
	ID                string
	raftID            raft.ServerID
	raftTransport     *raft.NetworkTransport
	raftConfig        *raft.Config
	raftLogstore      raft.LogStore
	raftFileSnapshots *raft.FileSnapshotStore

	mu sync.Mutex
	m  map[string]string // The key-value store for the system.
	gs gostore.ObjectStore

	raft *raft.Raft // The consensus mechanism

	logger hclog.Logger
}

// NewDefaultStore returns a new DefaultStore.
func NewDefaultStore(ID, raftDir, raftBind string) *DefaultStore {
	return &DefaultStore{
		m:        make(map[string]string),
		RaftDir:  raftDir,
		RaftBind: raftBind,
		ID:       ID,
		logger:   log.Named(fmt.Sprintf("[%s]", ID)),
	}
}

func (s *DefaultStore) Start() error {
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.ID)
	s.raftConfig = config
	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.RaftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}
	s.raftTransport = transport
	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}
	s.raftFileSnapshots = snapshots
	basePath := filepath.Join(slug.Make(s.ID), s.RaftDir)
	if err = os.MkdirAll(basePath, fs.FileMode(int(0777))); err != nil {
		return err
	}

	badgerdir := fmt.Sprintf("%s/badger", basePath)

	gs, err := badger.NewWithIndex(badgerdir, badgerdir+"_indexer")
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	s.gs = gs
	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, (*raftBadger)(s), (*raftBadger)(s), (*raftBadger)(s), snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	log.Info("open", s.ID, "raft", ra)
	s.raft = ra
	return nil
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
// localID should be the server identifier for this node.
func (s *DefaultStore) Open(enableSingle bool) error {
	if enableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      s.raftConfig.LocalID,
					Address: s.raftTransport.LocalAddr(),
				},
			},
		}
		s.raft.BootstrapCluster(configuration)
	}

	return nil
}

// Bootstrap bootstraps a cluster of known nodes
func (s *DefaultStore) Bootstrap(nodes [][]string) error {
	servers := make([]raft.Server, len(nodes))
	for i, n := range nodes {
		servers[i] = raft.Server{ID: raft.ServerID(n[0]), Address: raft.ServerAddress(n[1])}
	}
	s.logger.Info("Bootstraping %v", servers)
	f := s.raft.BootstrapCluster(raft.Configuration{Servers: servers})
	if err := f.Error(); err != nil {
		return err
	}
	return nil
}

// Leave cluster
func (s *DefaultStore) Leave(peerId string) error {
	if s.raft.State() != raft.Leader {
		return errors.New("not the leader")
	}
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	future := s.raft.RemoveServer(raft.ServerID(peerId), 0, 0)
	if err := future.Error(); err != nil {
		return err
	}
	return nil
}

// Set sets the value for the given key.
func (s *DefaultStore) Set(key, store string, value interface{}) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:    "set",
		Key:   key,
		Store: store,
		Value: value,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	if err := f.Error(); err != nil {
		return err
	}
	return nil
}

// Delete deletes the given key.
func (s *DefaultStore) Delete(key, store string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:    "delete",
		Key:   key,
		Store: store,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	if err := f.Error(); err != nil {
		return err
	}
	return nil
}

// Join joins a node, identified by nodeID and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (s *DefaultStore) Join(nodeID, addr string) error {
	s.logger.Debug("received join request for remote node as [%s]%s", nodeID, addr)

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	s.logger.Debug("node at %s joined successfully", addr)
	return nil
}

func (s *DefaultStore) GetConfiguration() raft.Configuration {
	future := s.raft.GetConfiguration()

	if err := future.Error(); err != nil {

		return raft.Configuration{}

	}

	return future.Configuration()

}

// IsLeader is this node the leader
func (s *DefaultStore) IsLeader() bool {
	if s.raft.State() != raft.Leader {
		return false
	}
	return true
}

func (s *DefaultStore) Leader() raft.ServerAddress {
	return s.raft.Leader()
}

func (s *DefaultStore) GetRaft() *raft.Raft {
	return s.raft
}

func (s *DefaultStore) Stats() map[string]string {
	return s.raft.Stats()
}

// Close the store
func (s *DefaultStore) Close() {
	s.gs.Close()
}

func (s *DefaultStore) DataStore() gostore.ObjectStore {
	return s.gs
}

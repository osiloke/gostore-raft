package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	common "github.com/osiloke/gostore-common"
)

var (
	// ErrNotOpen is returned when a Store is not open.
	ErrNotOpen = errors.New("store not open")

	// ErrNotLeader is returned when a node attempts to execute a leader-only
	// operation.
	ErrNotLeader = errors.New("not leader")

	// ErrStaleRead is returned if the executing the query would violate the
	// requested freshness.
	ErrStaleRead = errors.New("stale read")

	// ErrOpenTimeout is returned when the Store does not apply its initial
	// logs within the specified time.
	ErrOpenTimeout = errors.New("timeout waiting for initial logs application")

	// ErrInvalidBackupFormat is returned when the requested backup format
	// is not valid.
	ErrInvalidBackupFormat = errors.New("invalid backup format")
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	observerChanLen     = 50

	// raftLogCacheSize is the maximum number of logs to cache in-memory.
	// This is used to reduce disk I/O for the recently committed entries.
	raftLogCacheSize    = 512
	connectionPoolCount = 5
	connectionTimeout   = 10 * time.Second
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
	Close(bool) error
	GetConfiguration() raft.Configuration
	Bootstrap(nodes [][]string) error
	Leader() raft.ServerAddress
	IsLeader() bool
	Replay() error
	IsRaftLogCreated() bool
	Leave(peerId string) error
	Stats() map[string]string
}

// Store is an interface generated for "github.com/osiloke/gostore_raft/store".Store.
type Store interface {
	Delete(string, string) error
	Set(string, string, interface{}) error
	DataStore() common.ObjectStore
}

// DefaultStore is a simple key-value store, where all changes are made via Raft consensus.
type DefaultStore struct {
	open          bool
	openT         time.Time // Timestamp when Store opens.
	raftReplaying bool

	RaftDir  string
	RaftBind string
	ID       string
	// raftID            raft.ServerID
	raftTransport     *raft.NetworkTransport
	raftConfig        *raft.Config
	raftLogstore      raft.LogStore
	raftFileSnapshots *raft.FileSnapshotStore
	gs                common.ObjectStore

	raft *raft.Raft // The consensus mechanism

	logger hclog.Logger

	// Raft changes observer
	leaderObserversMu sync.RWMutex
	// leaderObservers   []chan<- struct{}
	observerClose chan struct{}
	observerDone  chan struct{}
	observerChan  chan raft.Observation
	observer      *raft.Observer
	// observerWg        sync.WaitGroup
	ln Listener
}

// NewDefaultStore returns a new DefaultStore.
func NewDefaultStore(ln Listener, ID, raftDir, raftBind string, gs common.ObjectStore) *DefaultStore {
	return &DefaultStore{
		RaftDir:  raftDir,
		RaftBind: raftBind,
		ID:       ID,
		gs:       gs,
		logger: hclog.New(&hclog.LoggerOptions{
			Name:  ID,
			Level: hclog.LevelFromString("DEBUG"),
		}),
		ln: ln,
	}
}

func (s *DefaultStore) Start() (retErr error) {

	defer func() {
		if retErr == nil {
			s.open = true
		}
	}()

	if s.open {
		return fmt.Errorf("store already open")
	}
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.ID)
	config.SnapshotThreshold = 1024

	fsmStore := &raftBadger{s.logger, s.gs}
	s.raftConfig = config

	basePath := s.RaftDir

	store, err := raftboltdb.NewBoltStore(filepath.Join(basePath, "raft.dataRepo"))
	if err != nil {
		return err
	}

	// Wrap the store in a LogCache to improve performance.
	cacheStore, err := raft.NewLogCache(raftLogCacheSize, store)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshotStore, err := raft.NewFileSnapshotStore(basePath, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Setup Raft communication.
	// addr, err := net.ResolveTCPAddr("tcp", s.RaftBind)
	// if err != nil {
	// 	return err
	// }

	transport := raft.NewNetworkTransport(NewTransport(s.ln), connectionPoolCount, connectionTimeout, nil)

	// transport, err := raft.NewTCPTransport(s.RaftBind, addr, 3, 10*time.Second, os.Stdout)
	// if err != nil {
	// 	return err
	// }
	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, fsmStore, cacheStore, store, snapshotStore, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.logger.Info("open", "id", s.ID, "raft", ra)
	s.raftFileSnapshots = snapshotStore
	s.raftTransport = transport
	s.raft = ra
	s.raftLogstore = store
	// fsmStore.AllKeys()
	// Open the observer channels.
	s.observerChan = make(chan raft.Observation, observerChanLen)
	s.observer = raft.NewObserver(s.observerChan, false, func(o *raft.Observation) bool {
		_, isLeaderChange := o.Data.(raft.LeaderObservation)
		_, isFailedHeartBeat := o.Data.(raft.FailedHeartbeatObservation)
		return isLeaderChange || isFailedHeartBeat
	})

	// Register and listen for leader changes.
	s.raft.RegisterObserver(s.observer)
	s.observerClose, s.observerDone = s.observe()
	return nil
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
// localID should be the server identifier for this node.
func (s *DefaultStore) Open(enableSingle bool) error {

	s.openT = time.Now()
	s.logger.Debug("opening store")
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
	if !s.IsLeader() {
		return errors.New("not leader")
	}
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	future := s.raft.RemoveServer(raft.ServerID(peerId), 0, 0)
	return future.Error()
}

func (s *DefaultStore) Replay() error {
	if !s.IsLeader() {
		s.logger.Warn("replay can only be initiated on the leader")
		return nil
	}
	s.logger.Debug("Replay")
	return s.replay()
}

// Set sets the value for the given key.
func (s *DefaultStore) Set(key, store string, value interface{}) error {
	if !s.IsLeader() {
		return errors.New("not leader")
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
	if !s.IsLeader() {
		return errors.New("not leader")
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
	s.logger.Debug("received join request for remote node", "nodeID", nodeID, "addr", addr)
	if !s.IsLeader() {
		return errors.New("not leader")
	}

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However, if *both* the ID and the address are the same, then no
			// join is actually needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				// stats.Add(numIgnoredJoins, 1)
				// s.numIgnoredJoins++
				s.logger.Debug("node is already member of cluster, ignoring join request", "node", hclog.Fmt("%v", nodeID), "address", hclog.Fmt("%v", addr))
				return nil
			}

			if err := s.remove(nodeID); err != nil {
				s.logger.Error("failed to remove node", "node", hclog.Fmt("%v", nodeID), "err", hclog.Fmt("v", err))
				return err
			}
			// stats.Add(numRemovedBeforeJoins, 1)
			s.logger.Debug("removed node %s prior to rejoin with changed ID or address", nodeID)
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	if e := f.(raft.Future); e.Error() != nil {
		if e.Error() == raft.ErrNotLeader {
			return ErrNotLeader
		}
		return e.Error()
	}
	s.logger.Debug("node joined successfully", "addr", addr)
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
	return s.raft.State() == raft.Leader
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
func (s *DefaultStore) Close(wait bool) (retErr error) {
	defer func() {
		if retErr == nil {
			s.open = false
		}
	}()
	if !s.open {
		// Protect against closing already-closed resource, such as channels.
		return nil
	}

	close(s.observerClose)
	<-s.observerDone

	f := s.raft.Shutdown()
	if wait {
		if f.Error() != nil {
			return f.Error()
		}
	}
	s.gs.Close()

	return nil
}

func (s *DefaultStore) DataStore() common.ObjectStore {
	return s.gs
}

// remove removes the node, with the given ID, from the cluster.
func (s *DefaultStore) remove(id string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	f := s.raft.RemoveServer(raft.ServerID(id), 0, 0)
	if f.Error() != nil {
		if f.Error() == raft.ErrNotLeader {
			return ErrNotLeader
		}
		return f.Error()
	}

	return nil
}

func (s *DefaultStore) observe() (closeCh, doneCh chan struct{}) {
	closeCh = make(chan struct{})
	doneCh = make(chan struct{})

	go func() {
		defer close(doneCh)
		for {
			select {
			case o := <-s.observerChan:
				switch signal := o.Data.(type) {
				case raft.PeerObservation:
					s.logger.Debug("failed heartbeat", "id", signal.Peer.ID, "address", signal.Peer.Address)
				case raft.FailedHeartbeatObservation:
					// stats.Add(failedHeartbeatObserved, 1)

					// nodes, err := s.Nodes()
					// if err != nil {
					// 	s.logger.Printf("failed to get nodes configuration during reap check: %s", err.Error())
					// }
					// servers := Servers(nodes)
					id := string(signal.PeerID)
					dur := time.Since(signal.LastContact)
					s.logger.Debug("failed heartbeat", "id", id, "curation", dur)
					// isReadOnly, found := servers.IsReadOnly(id)
					// if !found {
					// 	s.logger.Printf("node %s is not present in configuration", id)
					// 	break
					// }

					// if (isReadOnly && s.ReapReadOnlyTimeout > 0 && dur > s.ReapReadOnlyTimeout) ||
					// 	(!isReadOnly && s.ReapTimeout > 0 && dur > s.ReapTimeout) {
					// 	pn := "voting node"
					// 	if isReadOnly {
					// 		pn = "non-voting node"
					// 	}
					// 	if err := s.remove(id); err != nil {
					// 		stats.Add(nodesReapedFailed, 1)
					// 		s.logger.Printf("failed to reap %s %s: %s", pn, id, err.Error())
					// 	} else {
					// 		stats.Add(nodesReapedOK, 1)
					// 		s.logger.Printf("successfully reaped %s %s", pn, id)
					// 	}
					// }
				case raft.LeaderObservation:
					s.leaderObserversMu.RLock()
					// for i := range s.leaderObservers {
					// 	select {
					// 	case s.leaderObservers[i] <- struct{}{}:
					// 		stats.Add(leaderChangesObserved, 1)
					// 	default:
					// 		stats.Add(leaderChangesDropped, 1)
					// 	}
					// }
					s.logger.Debug("leader observation")
					s.leaderObserversMu.RUnlock()
				}

			case <-closeCh:
				return
			}
		}
	}()
	return closeCh, doneCh
}

func (s *DefaultStore) IsRaftLogCreated() bool {
	return s.raft.LastIndex() == 0
}

func (s *DefaultStore) GetLastAppliedKey() string {
	lastAppliedIndex := s.raft.LastIndex()
	l := &raft.Log{}
	err := s.raftLogstore.GetLog(lastAppliedIndex, l)
	if err != nil {
		// Handle error
		panic(err)
	}
	return string(string(l.Data))
}

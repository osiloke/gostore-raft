package node

import (
	"context"
	"log"
	"os"

	"github.com/osiloke/gostore"
	"github.com/osiloke/gostore_raft/service"
	"github.com/osiloke/gostore_raft/store"
)

// NewNode creates a new node
func NewNode(nodeID, advertiseName, raftAddr, raftDir string, gs gostore.ObjectStore) *Node {
	raftStore := store.NewDefaultStore(nodeID, raftDir, raftAddr, gs)
	srv := service.New(raftAddr, nodeID, advertiseName, raftStore, raftStore)

	logger := log.New(os.Stderr, "["+nodeID+"] ", log.LstdFlags)
	return &Node{srv: srv, raftStore: raftStore, logger: logger}
}

func NewExpectNode(nodeID, advertiseName, raftAddr, raftDir string, expect int, gs gostore.ObjectStore) *Node {
	raftStore := store.NewDefaultStore(nodeID, raftDir, raftAddr, gs)
	srv := service.New(raftAddr, nodeID, advertiseName, raftStore, raftStore)

	logger := log.New(os.Stderr, "["+nodeID+"] ", log.LstdFlags)
	return &Node{srv: srv, raftStore: raftStore, logger: logger, expect: expect}
}

// Node a node represents a store and its various raft foo
type Node struct {
	srv       *service.Service
	raftStore store.RaftStore
	cancel    context.CancelFunc
	logger    *log.Logger
	expect    int
}

// Start a node. Join existing nodes if possible
func (n *Node) Start(ctx context.Context, reload bool) error {
	if err := n.raftStore.Start(); err != nil {
		return err
	}
	if err := n.srv.Start(); err != nil {
		return err
	}
	if n.expect == 0 {
		srvs, err := n.srv.ListServices()
		if err != nil {
			n.logger.Println("Unable to get existing nodes " + err.Error())
			return err
		}
		count := len(srvs[0].Nodes)
		n.logger.Printf("existing nodes - %v", count)
		err = n.raftStore.Open(count <= 1)
		if err != nil {
			return err
		}
		// <-time.After(200 * time.Millisecond)
		if err := n.srv.Join(); err != nil {
			n.logger.Println("[WARN] unable to join any leaders")
			//try to bootstrap if registry is 3
			return err
		}
		if err := n.raftStore.Replay(); err != nil {
			return err
		}
		return nil
	}
	err := n.raftStore.Open(false)
	if err != nil {
		return err
	}
	return n.srv.Bootstrap(n.expect)

}

// Stop a node
func (n *Node) Stop() error {
	if err := n.srv.Leave(); err != nil {
		return err
	}
	if err := n.srv.Stop(); err != nil {
		return err
	}
	n.Close()
	return nil

}

// Close close this node service
func (n *Node) Close() {
	n.logger.Println("[INFO] closing node")
	if err := n.raftStore.Close(true); err != nil {
		log.Printf("failed to close store: %s", err.Error())
	}
	if n.cancel != nil {
		n.cancel()
	}
}

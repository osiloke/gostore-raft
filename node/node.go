package node

import (
	"context"
	"log"
	"os"

	"github.com/osiloke/gostore_raft/service"
	"github.com/osiloke/gostore_raft/store"
)

//NewNode creates a new node
func NewNode(nodeID, advertiseName, raftAddr, raftDir string) *Node {
	raftStore := store.NewDefaultStore(nodeID, raftDir, raftAddr)
	srv := service.New(raftAddr, nodeID, advertiseName, raftStore, raftStore)

	logger := log.New(os.Stderr, "["+nodeID+"] ", log.LstdFlags)
	return &Node{srv: srv, raftStore: raftStore, logger: logger}
}

func NewExpectNode(nodeID, advertiseName, raftAddr, raftDir string, expect int) *Node {
	raftStore := store.NewDefaultStore(nodeID, raftDir, raftAddr)
	srv := service.New(raftAddr, nodeID, advertiseName, raftStore, raftStore)

	logger := log.New(os.Stderr, "["+nodeID+"] ", log.LstdFlags)
	return &Node{srv: srv, raftStore: raftStore, logger: logger, expect: expect}
}

//Node a node represents a store and its various raft foo
type Node struct {
	srv       *service.Service
	ctx       context.Context
	raftStore store.RaftStore
	cancel    context.CancelFunc
	logger    *log.Logger
	expect    int
}

func (n *Node) run() error {
	ctx, cancel := context.WithCancel(context.Background())
	n.ctx = ctx
	n.cancel = cancel
	go n.srv.Run(ctx)
	if err := n.srv.Join(); err != nil {
		return err
	}
	return nil
}

//Start a node. Join existing nodes if possible
func (n *Node) Start(ctx context.Context) error {
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
	// n.raftStore.Close()
	if err := n.srv.Stop(); err != nil {
		return err
	}
	// if err := n.srv.Leave(); err != nil {
	// 	return err
	// }
	return nil

}

//Close close this node service
func (n *Node) Close() {
	if n.cancel != nil {
		n.cancel()
	}
}

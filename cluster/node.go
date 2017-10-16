package cluster

import (
	"context"
	"log"

	"github.com/osiloke/gostore_raft/service"
	"github.com/osiloke/gostore_raft/store"
)

//NewNode creates a new node
func NewNode(nodeID, advertiseName, raftAddr, raftDir string) *Node {
	raftStore := store.NewDefaultStore(nodeID, raftDir, raftAddr)
	srv := service.New(raftAddr, nodeID, advertiseName, raftStore)
	services, err := srv.ListServices()
	if err != nil {
		log.Println("Unable to get existing nodes " + err.Error())
	}
	err = raftStore.Open(len(services) == 0)
	if err != nil {
		panic(err)
	}
	return &Node{srv: srv, raftStore: raftStore}
}

//Node a node represents a store and its various raft foo
type Node struct {
	srv       *service.Service
	ctx       context.Context
	raftStore store.RaftStore
	cancel    context.CancelFunc
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

//Close close this node service
func (n *Node) Close() {
	if n.cancel != nil {
		n.cancel()
	}
}

package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/osiloke/gostore_raft/node"
)

func main() {
	var (
		nodeID, clusterName, raftDir, raftAddr string
	)
	flag.StringVar(&nodeID, "nodeID", "node0", "node id")
	flag.StringVar(&clusterName, "clusterName", "gostore.raft", "cluster name")
	flag.StringVar(&raftDir, "raftDir", "./.raft", "raft folder")
	flag.StringVar(&raftAddr, "raftAddr", "127.0.0.1:5000", "raft address")
	flag.Parse()

	nd := node.NewNode(nodeID, clusterName, raftAddr, raftDir)
	log.Printf("created %s", nodeID)
	log.Printf("starting %s", nodeID)

	ctx, cancel := context.WithCancel(context.Background())
	go nd.Start(ctx)
	defer cancel()
	defer nd.Stop()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL)
	select {
	case <-ctx.Done():
		log.Println("context ended")
		break
	case <-ch:
		log.Println("received os signal")
		break
	}
}

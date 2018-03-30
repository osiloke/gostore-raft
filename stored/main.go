package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/micro/cli"
	"github.com/micro/go-micro/cmd"
	"github.com/osiloke/gostore_raft/node"
)

func main() {
	var (
		nodeID, clusterName, raftDir, raftAddr string
		expect                                 int
	)
	cmd.DefaultCmd.App().Flags = append(
		cmd.DefaultCmd.App().Flags,
		cli.StringFlag{
			Name:        "nodeID",
			EnvVar:      "NODE_ID",
			Usage:       "node id",
			Value:       "node0",
			Destination: &nodeID,
		}, cli.StringFlag{
			Name:        "clusterName",
			EnvVar:      "CLUSTER_NAME",
			Usage:       "gostore.raft",
			Value:       "gostore.raft",
			Destination: &clusterName,
		}, cli.StringFlag{
			Name:        "raftDir",
			EnvVar:      "RAFT_DIR",
			Usage:       "./.raft",
			Value:       "./.raft",
			Destination: &raftDir,
		}, cli.StringFlag{
			Name:        "raftAddr",
			EnvVar:      "RAFT_ADDR",
			Usage:       "raft addr",
			Value:       "127.0.0.1:5000",
			Destination: &raftAddr,
		}, cli.IntFlag{
			Name:        "bootstrap-expect",
			EnvVar:      "BOOTSTRAP_EXPECT",
			Usage:       "if this is greater than 0, this node will only bootstrap when n nodes are available, this has to be an odd number greater than 1",
			Value:       0,
			Destination: &expect,
		})
	cmd.Init(cmd.Name("gostore.node"))
	var nd *node.Node
	if expect > 0 {
		nd = node.NewExpectNode(nodeID, clusterName, raftAddr, raftDir, expect)
	} else {
		nd = node.NewNode(nodeID, clusterName, raftAddr, raftDir)
	}
	log.Printf("created %s", nodeID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := nd.Start(ctx)
	if err != nil {
		log.Println(err.Error())
		return
	}
	log.Printf("started %s", nodeID)
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
	log.Printf("stopping %s", nodeID)
}

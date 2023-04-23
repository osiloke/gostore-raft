package main

import (
	"context"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/gosimple/slug"
	"github.com/osiloke/gostore-contrib/badger"
	"github.com/osiloke/gostore_raft/node"
	"github.com/urfave/cli/v2"
	"go-micro.dev/v4/util/cmd"
)

func main() {
	var (
		nodeID, clusterName, raftDir, raftAddr string
		expect                                 int
		reload                                 bool
	)
	cmd.DefaultCmd.App().Flags = append(
		cmd.DefaultCmd.App().Flags,
		&cli.StringFlag{
			Name:        "nodeID",
			EnvVars:     []string{"NODE_ID"},
			Usage:       "node id",
			Value:       "node0",
			Destination: &nodeID,
		},
		&cli.StringFlag{
			Name:        "clusterName",
			EnvVars:     []string{"CLUSTER_NAME"},
			Usage:       "gostore.raft",
			Value:       "go.micro.raft",
			Destination: &clusterName,
		},
		&cli.StringFlag{
			Name:        "raftDir",
			EnvVars:     []string{"RAFT_DIR"},
			Usage:       "./.raft",
			Value:       "./.raft",
			Destination: &raftDir,
		}, &cli.StringFlag{
			Name:        "raftAddr",
			EnvVars:     []string{"RAFT_ADDR"},
			Usage:       "raft addr",
			Value:       "127.0.0.1:5000",
			Destination: &raftAddr,
		}, &cli.IntFlag{
			Name:        "bootstrap-expect",
			EnvVars:     []string{"BOOTSTRAP_EXPECT"},
			Usage:       "if this is greater than 0, this node will only bootstrap when n nodes are available, this has to be an odd number greater than 1",
			Value:       0,
			Destination: &expect,
		},
		&cli.BoolFlag{
			Name:        "reload",
			EnvVars:     []string{"RELOAD"},
			Usage:       "reload",
			Value:       false,
			Destination: &reload,
		},
	)

	cmd.Init(cmd.Name("gostore.node"))
	var nd *node.Node

	basePath := filepath.Join(slug.Make(nodeID), raftDir)
	if err := os.MkdirAll(basePath, fs.FileMode(int(0777))); err != nil {
		panic(err)
	}

	db, err := badger.NewWithIndex(basePath, "moss")
	if err != nil {
		panic(err)
	}

	if expect > 0 {
		nd = node.NewExpectNode(nodeID, clusterName, raftAddr, basePath, expect, db)
	} else {
		nd = node.NewNode(nodeID, clusterName, raftAddr, basePath, db)
	}
	log.Printf("created %s", nodeID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		err := nd.Start(ctx, reload)
		if err != nil {
			log.Println("failed starting", err.Error())
			cancel()
			return
		}
	}()
	log.Printf("started %s", nodeID)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	select {
	case <-ctx.Done():
		log.Println("context ended")
		break
	case <-ch:
		log.Println("received os signal")
		break
	}
	if err := nd.Stop(); err != nil {
		log.Printf("failed stopping node %v", err)
	} else {
		log.Println("stopped node")
	}
}

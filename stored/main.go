package main

import (
	"context"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/blevesearch/bleve/v2"
	"github.com/gosimple/slug"
	"github.com/osiloke/gostore-contrib/badger"
	"github.com/osiloke/gostore_raft/node"
	"github.com/urfave/cli/v2"
	"go-micro.dev/v4/util/cmd"
)

func main() {
	cfg := &node.Config{}
	cmd.DefaultCmd.App().Flags = append(
		cmd.DefaultCmd.App().Flags,
		&cli.StringFlag{
			Name:        "nodeID",
			EnvVars:     []string{"NODE_ID"},
			Usage:       "node id",
			Value:       "node0",
			Destination: &cfg.NodeID,
		},
		&cli.StringFlag{
			Name:        "clusterName",
			EnvVars:     []string{"ADVERTISE_NAME"},
			Usage:       "gostore.raft",
			Value:       "go.micro.raft",
			Destination: &cfg.AdvertiseName,
		},
		&cli.StringFlag{
			Name:        "raftDir",
			EnvVars:     []string{"RAFT_DIR"},
			Usage:       "./.raft",
			Value:       "./.raft",
			Destination: &cfg.RaftDir,
		}, &cli.StringFlag{
			Name:        "raftAddr",
			EnvVars:     []string{"RAFT_ADDR"},
			Usage:       "raft addr",
			Value:       "127.0.0.1:5000",
			Destination: &cfg.RaftAddr,
		}, &cli.IntFlag{
			Name:        "bootstrap-expect",
			EnvVars:     []string{"BOOTSTRAP_EXPECT"},
			Usage:       "if this is greater than 0, this node will only bootstrap when n nodes are available, this has to be an odd number greater than 1",
			Value:       0,
			Destination: &cfg.Expect,
		}, &cli.StringFlag{
			Name:        "node-ca-cert",
			EnvVars:     []string{"NODE_CA_CERT"},
			Usage:       "specify path to ca cert",
			Value:       "",
			Destination: &cfg.NodeX509CACert,
		}, &cli.StringFlag{
			Name:        "node-cert",
			EnvVars:     []string{"NODE_CERT"},
			Usage:       "specify path to cert",
			Value:       "",
			Destination: &cfg.NodeX509Cert,
		}, &cli.StringFlag{
			Name:        "node-key",
			EnvVars:     []string{"NODE_KEY"},
			Usage:       "specify path to key",
			Value:       "",
			Destination: &cfg.NodeX509Key,
		}, &cli.BoolFlag{
			Name:        "node-no-verify",
			EnvVars:     []string{"NODE_NO_VERIFY"},
			Usage:       "skip verifying node certs",
			Value:       false,
			Destination: &cfg.NoNodeVerify,
		}, &cli.BoolFlag{
			Name:        "node-verify-client",
			EnvVars:     []string{"NODE_VERIFY_CLIENT"},
			Usage:       "skip verifying node certs",
			Value:       false,
			Destination: &cfg.NodeVerifyClient,
		},
	)

	cmd.Init(cmd.Name("gostore.node"))
	var nd *node.Node

	raftDir := filepath.Join(slug.Make(cfg.NodeID), cfg.RaftDir)
	if err := os.MkdirAll(raftDir, fs.FileMode(int(0777))); err != nil {
		panic(err)
	}
	indexMapping := bleve.NewIndexMapping()
	db, err := badger.NewWithIndex(raftDir, "moss", indexMapping)
	if err != nil {
		panic(err)
	}

	cfg.RaftDir = raftDir
	cfg.GoStore = db

	nd = node.NewNode(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		err := nd.Start(ctx)
		if err != nil {
			log.Println("failed starting", err.Error())
			cancel()
			return
		}
	}()
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

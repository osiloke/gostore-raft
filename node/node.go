package node

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/go-micro/plugins/v4/registry/nats"
	common "github.com/osiloke/gostore/common"
	"github.com/osiloke/gostore_raft/service"
	"github.com/osiloke/gostore_raft/store"
	"github.com/rqlite/rqlite/tcp"
	"go-micro.dev/v5/registry"
	"go-micro.dev/v5/server"
)

const (
	MuxRaftHeader    = 1
	MuxClusterHeader = 2
)

// Node a node represents a store and its various raft foo
type Node struct {
	srv       *service.Service
	raftStore store.RaftStore
	kvStore   store.Store
	cancel    context.CancelFunc
	logger    *log.Logger
	cfg       *Config
}

// NodeGoStore returns a gostore object store that overides save and delete to use the node's kv store
type NodeGoStore struct {
	common.ObjectStore
	node *Node
}

// NewNode creates a new node
func NewNode(cfg *Config) *Node {
	if cfg.Server == nil {
		r := server.DefaultOptions().Registry
		opts := server.DefaultOptions().Registry.Options()
		if r.String() == "nats" {
			r = nats.NewRegistry(
				registry.TLSConfig(opts.TLSConfig),
				registry.Timeout(500*time.Millisecond),
				registry.Addrs(opts.Addrs...),
				registry.Secure(opts.Secure),
				registry.Logger(opts.Logger))
		}
		serviceServer := server.NewServer(
			server.Name(cfg.AdvertiseName),
			server.Id(cfg.NodeID),
			server.Registry(r),
			server.Metadata(map[string]string{
				"ID":       cfg.NodeID,
				"raftAddr": cfg.RaftAddr,
			}),
			// server.Transport(server.DefaultOptions().Transport),
			// server.Registry(mdns.NewRegistry()),
		)

		cfg.Server = serviceServer
	}
	logger := log.New(os.Stderr, "["+cfg.NodeID+"] ", log.LstdFlags)
	return &Node{cfg: cfg, logger: logger}
}

func (n *Node) startNodeMux(cfg *Config, ln net.Listener) (*tcp.Mux, error) {
	var err error
	adv := tcp.NameAddress{
		Address: cfg.RaftAddr,
	}

	var mux *tcp.Mux
	if cfg.NodeX509Cert != "" {
		var b strings.Builder
		b.WriteString(fmt.Sprintf("enabling node-to-node encryption with cert: %s, key: %s",
			cfg.NodeX509Cert, cfg.NodeX509Key))
		if cfg.NodeX509CACert != "" {
			b.WriteString(fmt.Sprintf(", CA cert %s", cfg.NodeX509CACert))
		}
		if cfg.NodeVerifyClient {
			b.WriteString(", mutual TLS disabled")
		} else {
			b.WriteString(", mutual TLS enabled")
		}
		n.logger.Println(b.String())
		mux, err = tcp.NewTLSMux(ln, adv, cfg.NodeX509Cert, cfg.NodeX509Key, cfg.NodeX509CACert,
			cfg.NoNodeVerify, cfg.NodeVerifyClient)
	} else {
		mux, err = tcp.NewMux(ln, adv)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create node-to-node mux: %s", err.Error())
	}
	go mux.Serve()

	return mux, nil
}

// Start a node. Join existing nodes if possible
func (n *Node) Start(ctx context.Context) error {
	n.logger.Println("starting node ", n.cfg.AdvertiseName+"."+n.cfg.NodeID)
	muxLn, err := net.Listen("tcp", n.cfg.RaftAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %s", n.cfg.RaftAddr, err.Error())
	}
	mux, err := n.startNodeMux(n.cfg, muxLn)
	if err != nil {
		return err
	}
	ln := mux.Listen(MuxRaftHeader)
	raftStore := store.NewDefaultStore(ln, n.cfg.NodeID, n.cfg.RaftDir, n.cfg.RaftAddr, n.cfg.GoStore)
	srv := service.New(n.cfg.RaftAddr, n.cfg.NodeID, n.cfg.AdvertiseName, raftStore, raftStore, n.cfg.Server)
	n.raftStore = raftStore
	n.kvStore = (store.Store)(raftStore)
	n.srv = srv

	if err := raftStore.Start(); err != nil {
		return err
	}

	if err := srv.Start(); err != nil {
		return err
	}
	if n.cfg.Expect == 0 {
		srvs, err := n.srv.ListServices()
		if err != nil {
			n.logger.Println("Unable to get existing nodes " + err.Error())
			return err
		}
		if len(srvs) > 0 {
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
		}
		return nil
	}
	err = n.raftStore.Open(false)
	if err != nil {
		return err
	}
	return n.srv.Bootstrap(n.cfg.Expect)

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

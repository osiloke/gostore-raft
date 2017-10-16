package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/registry"
	"github.com/micro/go-micro/server"

	"github.com/micro/go-micro/registry/mdns"
	"github.com/osiloke/gostore_raft/service/handler"
	proto "github.com/osiloke/gostore_raft/service/proto/service"
	"github.com/osiloke/gostore_raft/store"
)

// New service
func New(raftAddr, nodeID, advertiseName string, raftStore store.RaftStore) *Service {
	serviceServer := server.NewServer(
		server.Name(advertiseName),
		server.Id(nodeID),
		server.Metadata(map[string]string{
			"ID":       nodeID,
			"raftAddr": raftAddr,
		}),
		server.Registry(mdns.NewRegistry()),
	)
	srv := &Service{raftAddr, nodeID, advertiseName, raftStore, serviceServer, log.New(os.Stderr, "["+nodeID+"] ", log.LstdFlags)}
	srv.setup()
	return srv
}

// Service handles registrations and discovery
type Service struct {
	raftAddr      string
	nodeID        string
	advertiseName string
	raftStore     store.RaftStore
	server        server.Server
	logger        *log.Logger
}

func (s *Service) setup() {
	// Register Handlers
	s.server.Handle(
		server.NewHandler(
			&handler.Service{Store: s.raftStore},
		),
	)
}

// ListServices list all nodes that are reachable under the advertise name
func (s *Service) ListServices() ([]*registry.Service, error) {
	return s.server.Options().Registry.GetService(s.advertiseName)
}

func (s *Service) join(srv *registry.Service, raftAddr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	c := client.NewClient(client.Registry(s.server.Options().Registry))
	// log.Println("join", fmt.Sprintf("%v", srv), "raftAddr", raftAddr)

	rsp := new(proto.Response)
	node := srv.Nodes[0]
	address := node.Address
	if node.Port > 0 {
		address = fmt.Sprintf("%s:%d", address, node.Port)
	}
	req := client.NewRequest(srv.Name, "Service.Join", &proto.Request{NodeID: s.nodeID, RaftAddr: raftAddr})
	err := c.CallRemote(ctx, address, req, rsp)
	return err
}

// Join is a convinience method for sending a join request to every service except the current one
func (s *Service) Join() error {
	if s.raftStore.IsLeader() {
		return errors.New("you are the leader")
	}
	//get all services available
	services, err := s.ListServices()
	if err != nil {
		return err
	}
	for _, srv := range services {
		s.logger.Printf("Attempting to join %s at %s", srv.Name, srv.Nodes[0].Metadata["raftAddr"])
		err := s.join(srv, s.raftAddr)
		if err == nil {
			s.logger.Printf("joined %s at %s", srv.Name, srv.Nodes[0].Metadata["raftAddr"])
			break
		} else {
			if err.Error() != "node is not the leader" {
				return err
			}
		}
	}
	return nil
}

// Run runs a node service
func (s *Service) Run(ctx context.Context) error {
	ch := make(chan os.Signal, 1)
	stop := make(chan bool, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL)

	if err := s.server.Start(); err != nil {
		return err
	}

	if err := s.server.Register(); err != nil {
		s.server.Stop()
		return err
	}
	select {
	case <-ctx.Done():
		log.Println("context ended")
		stop <- true
	case <-stop:
		log.Println("stopping")
		break
	case <-ch:
		log.Println("received os signal")
		stop <- true
	}
	if err := s.server.Deregister(); err != nil {
		return err
	}

	return s.server.Stop()
}

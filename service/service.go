package service

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/matryer/try"
	"go-micro.dev/v4/client"
	"go-micro.dev/v4/registry"
	"go-micro.dev/v4/server"

	"github.com/osiloke/gostore_raft/service/handler"
	proto "github.com/osiloke/gostore_raft/service/proto/service"
	"github.com/osiloke/gostore_raft/store"
)

// New service
func New(raftAddr, nodeID, advertiseName string, raftStore store.RaftStore, dataStore store.Store) *Service {
	serviceServer := server.NewServer(
		server.Name(advertiseName),
		server.Id(nodeID),
		server.Metadata(map[string]string{
			"ID":       nodeID,
			"raftAddr": raftAddr,
		}),
		// server.Registry(mdns.NewRegistry()),
	)
	logger := log.New(os.Stderr, "["+nodeID+"] ", log.LstdFlags)
	srv := &Service{raftAddr, nodeID, advertiseName, raftStore, dataStore, serviceServer, logger}
	srv.setup()
	return srv
}

// NewWithServer service
func NewWithServer(raftAddr, nodeID, advertiseName string, raftStore store.RaftStore, dataStore store.Store, microService server.Server) *Service {
	logger := log.New(os.Stderr, "["+nodeID+"] ", log.LstdFlags)
	srv := &Service{raftAddr, nodeID, advertiseName, raftStore, dataStore, microService, logger}
	srv.setup()
	return srv
}

// Service handles registrations and discovery
type Service struct {
	raftAddr      string
	nodeID        string
	advertiseName string
	raftStore     store.RaftStore
	store         store.Store
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
	s.server.Handle(
		server.NewHandler(
			&handler.Store{Store: s.store},
		),
	)
}

// ListServices list all nodes that are reachable under the advertise name
func (s *Service) ListServices() ([]*registry.Service, error) {
	return s.server.Options().Registry.GetService(s.advertiseName)
}

func (s *Service) callJoin(srvName string, node *registry.Node, raftAddr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	c := client.NewClient(client.Registry(s.server.Options().Registry))
	// log.Println("join", fmt.Sprintf("%v", srv), "raftAddr", raftAddr)

	rsp := new(proto.Response)
	address := node.Address
	// if node.Port > 0 {
	// 	address = fmt.Sprintf("%s:%d", address, node.Port)
	// }
	req := client.NewRequest(srvName, "Service.Join", &proto.Request{NodeID: s.nodeID, RaftAddr: raftAddr})
	err := c.Call(ctx, req, rsp, client.WithRetries(3), client.WithAddress(address))
	return err
}

func (s *Service) join() error {
	//get all services available
	services, err := s.ListServices()
	if err != nil {
		return err
	}
	if len(services) == 0 {
		return errors.New("no services found")
	}
	srv := services[0]
	nodes := srv.Nodes
	count := len(nodes)
	s.logger.Printf("found %d service nodes", count)

	for _, node := range nodes {
		raftAddr := node.Metadata["raftAddr"]
		if raftAddr == s.raftAddr {
			s.logger.Println("skipping self")
			continue
		}
		s.logger.Printf("Attempting to join cluster %s at %s", srv.Name, raftAddr)
		err := s.callJoin(srv.Name, node, s.raftAddr)
		if err == nil {
			s.logger.Printf("joined cluster %s at %s", srv.Name, raftAddr)
			return nil
		}
		s.logger.Printf("cannot join %s - %s", raftAddr, err.Error())
		if !strings.Contains(err.Error(), "not the leader") {
			return err
		}
	}
	return errors.New("unable to join a node")
}

func (s *Service) bootstrap(expect int) error {
	services, err := s.ListServices()
	if err != nil {
		return err
	}
	if len(services) == 0 {
		return errors.New("no services found")
	}
	srv := services[0]
	nodes := srv.Nodes
	count := len(nodes)
	// bootstrap expect 3
	if count == expect {
		s.logger.Printf("attempting to bootstra to %d service nodes", count)
		rnodes := make([][]string, count)
		for i, node := range nodes {
			ID := node.Metadata["ID"]
			raftAddr := node.Metadata["raftAddr"]
			rnodes[i] = []string{ID, raftAddr}
		}
		return s.raftStore.Bootstrap(rnodes)
	}
	return errors.New("waiting")
}

func (s *Service) Bootstrap(expect int) error {

	// if s.raftStore.IsLeader() {
	// 	return errors.New("you are the leader")
	// }
	//this needs to be retried, in case a leader is still staging
	err := try.Do(func(attempt int) (bool, error) {

		if s.raftStore.IsLeader() {
			return true, nil
		}
		s.logger.Printf("bootstrap attempt #%d", attempt)
		var err error
		err = s.bootstrap(expect)
		if err != nil {
			s.logger.Printf("bootstrap error %v", err.Error())
			<-time.After(1 * time.Second)
			if strings.Contains(err.Error(), "waiting") {
				s.logger.Printf("waiting for %d nodes to come up", expect)
				return true, err
			}
		}
		return false, err // try 5 times
	})
	return err
}

// Join is a convinience method for sending a join request to every service except the current one
func (s *Service) Join() error {

	// if s.raftStore.IsLeader() {
	// 	return errors.New("you are the leader")
	// }
	//this needs to be retried, in case a leader is still staging
	err := try.Do(func(attempt int) (bool, error) {

		if s.raftStore.IsLeader() {
			return true, nil
		}
		s.logger.Printf("join attempt #%d", attempt)
		var err error
		err = s.join()
		if err != nil {
			<-time.After(1 * time.Second)
		}
		return attempt < 5, err // try 5 times
	})
	return err
}

// Start this service
func (s *Service) Start() error {
	if err := s.server.Start(); err != nil {
		return err
	}

	// if err := s.server.Register(); err != nil {
	// 	s.server.Stop()
	// 	return err
	// }
	return nil
}

//Stop this service
func (s *Service) Stop() error {
	if err := s.server.Stop(); err != nil {
		return err
	}

	// if err := s.server.Deregister(); err != nil {
	// 	return err
	// }
	return nil
}

// Run runs a service which listens for raft and store requests
func (s *Service) Run(ctx context.Context) error {
	ch := make(chan os.Signal, 1)
	stop := make(chan bool, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL)

	if err := s.Start(); err != nil {
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

	if err := s.Stop(); err != nil {
		log.Println("unable to stop")
		return err
	}
	return nil
}

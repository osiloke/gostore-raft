// go generate protoc --proto_path=.  --go_out=. store.proto
package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/matryer/try"
	"go-micro.dev/v4/client"
	"go-micro.dev/v4/registry"
	"go-micro.dev/v4/server"

	"github.com/osiloke/gostore_raft/handler"
	proto "github.com/osiloke/gostore_raft/service/proto/service"
	storeProto "github.com/osiloke/gostore_raft/service/proto/store"
	"github.com/osiloke/gostore_raft/store"
)

// New service
func New(raftAddr, nodeID, advertiseName string, raftStore store.RaftStore, dataStore store.Store, serviceServer server.Server) *Service {
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

func (s *Service) Store() store.Store {
	return s.store
}

func (s *Service) setup() {
	// Register Handlers
	s.server.Handle(
		server.NewHandler(
			&handler.Service{Store: s.raftStore},
		),
	)
	storeProto.RegisterStoreHandler(s.server, handler.NewStore(s.advertiseName, s.store, s.raftStore))
}

// ListServices list all nodes that are reachable under the advertise name
func (s *Service) ListServices() ([]*registry.Service, error) {
	fmt.Println("registry timeout", s.server.Options().Registry.Options().Timeout)
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

func (s *Service) callLeave(srvName string, node *registry.Node, raftAddr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	c := client.NewClient(client.Registry(s.server.Options().Registry))
	rsp := new(proto.Response)
	address := node.Address
	req := client.NewRequest(srvName, "Service.Leave", &proto.Request{NodeID: s.nodeID, RaftAddr: raftAddr})
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
		s.logger.Printf("Attempting to join cluster [%s] via leader %s at %s", srv.Name, node.Id, raftAddr)
		err := s.callJoin(srv.Name, node, s.raftAddr)
		if err == nil {
			// set leader in service
			s.logger.Printf("joined cluster [%s] leader %s at %s", srv.Name, node.Id, raftAddr)
			return nil
		}
		s.logger.Printf("cannot join %s - %s", raftAddr, err.Error())
		if !strings.Contains(err.Error(), "not leader") {
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
		s.logger.Printf("attempting to bootstrap to %d service nodes", count)
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
		err := s.bootstrap(expect)
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
	//this needs to be retried, in case a leader is still staging
	err := try.Do(func(attempt int) (bool, error) {
		s.logger.Printf("join attempt #%d", attempt)
		if s.raftStore.IsLeader() {
			s.logger.Printf("join attempt resolved - this is the leader")
			// check
			// if !s.raftStore.IsRaftLogCreated() {
			// 	s.logger.Printf("reapplying existing fsm")
			// 	if err := s.raftStore.ReapplyFSM(); err != nil {
			// 		return false, err
			// 	}
			// }
			return true, nil
		}

		err := s.join()
		if err != nil {
			<-time.After(1 * time.Second)
		}
		return attempt < 10, err // try 50 times
	})
	return err
}

// Start this service
func (s *Service) Start() error {
	if err := s.server.Start(); err != nil {
		return err
	}
	return nil
}

// Stop this service
func (s *Service) Stop() error {
	if err := s.server.Stop(); err != nil {
		return err
	}

	// if err := s.server.Deregister(); err != nil {
	// 	return err
	// }
	return nil
}

func (s *Service) Leave() error {
	if s.raftStore.IsLeader() {
		return s.raftStore.Leave(s.raftAddr)
	}
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
		s.logger.Printf("Attempting to leave cluster [%s] via leader %s at %s", srv.Name, node.Id, raftAddr)
		err := s.callLeave(srv.Name, node, s.raftAddr)
		if err == nil {
			// set leader in service
			s.logger.Printf("left cluster [%s] leader %s at %s", srv.Name, node.Id, raftAddr)
			return nil
		}
		s.logger.Printf("cannot leave %s - %s", raftAddr, err.Error())
	}
	return errors.New("unable to leave cluster")
}

// Run runs a service which listens for raft and store requests
func (s *Service) Run(ctx context.Context) error {

	err := s.Start()
	if err != nil {
		panic(err)
		// return err
	}

	return err
}

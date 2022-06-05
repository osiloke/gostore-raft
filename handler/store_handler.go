package handler

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	proto "github.com/osiloke/gostore_raft/service/proto/store"
	"github.com/osiloke/gostore_raft/store"
	"go-micro.dev/v4/client"
	"go-micro.dev/v4/metadata"
	"go-micro.dev/v4/server"

	"golang.org/x/net/context"
)

//Store store handler for store requests
type Store struct {
	serviceName string
	store       store.Store
	raftStore   store.RaftStore
}

func NewStore(serviceName string, s store.Store, rs store.RaftStore) *Store {
	return &Store{serviceName, s, rs}
}

// TODO: should be able to fetch leader from registry
func (s *Store) forwardToLeader(ctx context.Context, resource string, req *proto.Request, rsp *proto.Response) error {
	configFuture := s.raftStore.GetConfiguration()
	for _, peer := range configFuture.Servers {
		if peer.Address == s.raftStore.Leader() {
			ctx2, cancel := context.WithTimeout(metadata.NewContext(context.Background(), map[string]string{
				"NodeID": string(peer.ID),
			}), 10*time.Second)
			defer cancel()
			nodeAdvertiseName := fmt.Sprintf("%s.%s", s.serviceName, peer.ID)
			log.Printf("forwarding %s to leader [%s] @ %s", resource, nodeAdvertiseName, peer.Address)
			c := client.NewClient(client.Wrap(NewNodeSelectorWrapper))
			req := client.NewRequest(s.serviceName, resource, req)
			return c.Call(ctx2, req, rsp, client.WithRetries(3))
		}
	}
	return errors.New("leader not found")
}

// Get a key from the store
func (s *Store) Get(ctx context.Context, req *proto.Request, rsp *proto.Response) error {
	log.Printf("%s - received get request", server.DefaultOptions().Id)
	val, err := s.store.Get(req.Key)
	if err != nil {
		return err
	}
	rsp.Key = req.Key
	rsp.Val = val
	return nil
}

// Set a key in the store
func (s *Store) Set(ctx context.Context, req *proto.Request, rsp *proto.Response) error {
	if err := s.store.Set(req.Key, req.Val); err != nil {
		// if err is not leader, then forward req to leader
		if strings.Contains(err.Error(), "not leader") {
			return s.forwardToLeader(ctx, "Store.Set", req, rsp)
		}
		return err
	}
	rsp.Key = req.Key
	rsp.Val = req.Val
	return nil
}

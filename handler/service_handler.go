package handler

import (
	"encoding/json"
	"errors"
	"log"

	service "github.com/osiloke/gostore_raft/service/proto/service"
	"github.com/osiloke/gostore_raft/store"
	"go-micro.dev/v5/server"

	"golang.org/x/net/context"
)

// Service a handler for raft requests
type Service struct {
	Store store.RaftStore
}

// Join a cluster
func (e *Service) Join(ctx context.Context, req *service.Request, rsp *service.Response) error {
	// md, _ := metadata.FromContext(ctx)
	// log.Printf("Received Service.Call request with metadata: %v", md)
	if e.Store == nil {
		return errors.New("store not initialized")
	}
	if err := e.Store.Join(req.NodeID, req.RaftAddr); err != nil {
		return err
	}
	rsp.Msg = server.DefaultOptions().Id + ": Accepted " + req.RaftAddr
	return nil
}

// Leave a cluster
func (e *Service) Leave(ctx context.Context, req *service.Request, rsp *service.Response) error {
	// md, _ := metadata.FromContext(ctx)
	log.Printf("Received Service.Leave request from %s", req.RaftAddr)
	if err := e.Store.Leave(req.NodeID); err != nil {
		return err
	}
	rsp.Msg = server.DefaultOptions().Id + ": Accepted " + req.RaftAddr
	return nil
}

// Stats a cluster
func (s *Service) Stats(ctx context.Context, req *service.Request, rsp *service.Response) error {
	v, _ := json.Marshal(s.Store.Stats())
	rsp.Msg = string(v)
	return nil
}

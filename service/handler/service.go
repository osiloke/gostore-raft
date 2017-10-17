package handler

import (
	"errors"

	"github.com/micro/go-micro/server"
	service "github.com/osiloke/gostore_raft/service/proto/service"
	"github.com/osiloke/gostore_raft/store"

	"golang.org/x/net/context"
)

//Service a handler for raft requests
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

//Leave a cluster
func (e *Service) Leave(ctx context.Context, req *service.Request, rsp *service.Response) error {
	// md, _ := metadata.FromContext(ctx)
	// log.Printf("Received Service.Call request with metadata: %v", md)
	if err := e.Store.Join(req.NodeID, req.RaftAddr); err != nil {
		return err
	}
	rsp.Msg = server.DefaultOptions().Id + ": Accepted " + req.RaftAddr
	return nil
}

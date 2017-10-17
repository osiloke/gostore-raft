package handler

import (
	"log"

	"github.com/micro/go-micro/server"
	proto "github.com/osiloke/gostore_raft/service/proto/store"
	"github.com/osiloke/gostore_raft/store"

	"golang.org/x/net/context"
)

//Store store handler for store requests
type Store struct {
	Store store.Store
}

// Get a key from the store
func (e *Store) Get(ctx context.Context, req *proto.Request, rsp *proto.Response) error {
	log.Printf("%s - received get request", server.DefaultOptions().Id)
	val, err := e.Store.Get(req.Key)
	if err != nil {
		return err
	}
	rsp.Key = req.Key
	rsp.Val = val
	return nil
}

// Set a key in the store
func (e *Store) Set(ctx context.Context, req *proto.Request, rsp *proto.Response) error {
	if err := e.Store.Set(req.Key, req.Val); err != nil {
		return err
	}
	rsp.Key = req.Key
	rsp.Val = req.Val
	return nil
}

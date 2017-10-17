package client

import (
	"context"

	"github.com/micro/go-micro/client"
	proto "github.com/osiloke/gostore_raft/service/proto/store"
)

// NewMicroClient creates a new micro client for a cluster at advertiseName
func NewMicroClient(advertiseName string) *MicroClient {
	microClient := proto.NewStoreClient(advertiseName, client.DefaultClient)
	return &MicroClient{
		advertiseName: advertiseName,
		client:        microClient,
	}
}

//MicroClient is a go-micro client which proxies gostore methods to a cluster of nodes in a go-micro ecosystem
type MicroClient struct {
	client        proto.StoreClient
	advertiseName string
}

// Get retrieves a key from a micro node
func (n *MicroClient) Get(key string) (string, error) {
	rsp, err := n.client.Get(context.Background(), &proto.Request{Key: key})
	if err != nil {
		return "", err
	}
	return rsp.Val, nil
}

// Set saves a key to a micro node
func (n *MicroClient) Set(key, val string) (string, error) {
	rsp, err := n.client.Set(context.Background(),
		&proto.Request{Key: key, Val: val},
		client.WithRetries(2))
	if err != nil {
		return "", err
	}
	return rsp.Val, nil
}

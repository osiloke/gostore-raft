package handler

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	service "github.com/osiloke/gostore_raft/service/proto/service"
	"github.com/osiloke/gostore_raft/store"
	assert "github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestService_Join(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := &service.Request{NodeID: "testNode", RaftAddr: "localhost:2001"}
	rsp := new(service.Response)

	defaultStore := store.NewDefaultStore()
	defaultStore.Open(true, "leaderTestNode")
	tmpDir, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir)

	defaultStore.RaftBind = "127.0.0.1:0"
	defaultStore.RaftDir = tmpDir

	s := Service{Store: defaultStore}
	<-time.After(3 * time.Second)
	err := s.Join(ctx, req, rsp)
	assert.Nil(t, err)
}

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

	req := &service.Request{NodeID: "node1", RaftAddr: "127.0.0.1:1"}
	rsp := new(service.Response)

	tmpDir, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir)
	defaultStore := store.NewDefaultStore("node0", tmpDir, "127.0.0.1:0")
	defaultStore.Open(true)

	//start a raft service handler
	s := Service{Store: defaultStore}
	<-time.After(3 * time.Second)
	err := s.Join(ctx, req, rsp)
	assert.Nil(t, err)
}

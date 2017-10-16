package service

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/osiloke/gostore_raft/store"
	assert "github.com/stretchr/testify/assert"
)

func makeService(nodeID string, raftAddr string, single bool, t *testing.T) (context.CancelFunc, *Service) {
	tmpDir, _ := ioutil.TempDir("", nodeID)
	defer os.RemoveAll(tmpDir)
	node0Store := store.NewDefaultStore(nodeID, tmpDir, raftAddr)
	err := node0Store.Open(single)
	assert.Nil(t, err)

	srv := New(raftAddr, nodeID, "gostoreraft.test.nodes", node0Store)
	ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()
	go func(srv *Service, tmpDir string) {
		defer os.RemoveAll(tmpDir)
		err := srv.Run(ctx)
		assert.Nil(t, err)
	}(srv, tmpDir)
	return cancel, srv
}
func TestRun(t *testing.T) {
	nodeID := "node0"
	raftAddr := "127.0.0.1:0"
	tmpDir, _ := ioutil.TempDir("", nodeID)
	defer os.RemoveAll(tmpDir)
	node0Store := store.NewDefaultStore(nodeID, tmpDir, "127.0.0.1:0")

	err := node0Store.Open(true)
	assert.Nil(t, err)
	srv := New(raftAddr, nodeID, "gostoreraft.test.nodes", node0Store)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = srv.Run(ctx)
	assert.Nil(t, err)
}
func TestJoin(t *testing.T) {
	//startup two services, the first on will be the leader
	//the second will try to join the first one

	cancelNode1, _ := makeService("srv1", "127.0.0.1:0", true, t)
	defer cancelNode1()
	// <-time.After(3 * time.Second)
	//join

	// <-time.After(3 * time.Second)

	cancelNode2, srv2 := makeService("srv2", "127.0.0.1:1", false, t)
	defer cancelNode2()
	go func() {
		err := srv2.Join()
		assert.Nil(t, err)
	}()

	<-time.After(5 * time.Second)

	cancelNode3, srv3 := makeService("srv3", "127.0.0.1:2", false, t)
	defer cancelNode3()
	go func() {
		err := srv3.Join()
		assert.Nil(t, err)
	}()

	<-time.After(10 * time.Second)
	// <-time.After(30 * time.Second)

}

package node

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewNode(t *testing.T) {
	nodeID := "node0"
	raftAddr := "127.0.0.1:0"
	tmpDir, _ := ioutil.TempDir("", nodeID)
	defer os.RemoveAll(tmpDir)
	node := NewNode(nodeID, "node.test", raftAddr, tmpDir)
	assert.NotNil(t, node)
}
func Test_StartSingleNode(t *testing.T) {
	nodeID := "node0"
	raftAddr := "127.0.0.1:0"
	tmpDir, _ := ioutil.TempDir("", nodeID)
	defer os.RemoveAll(tmpDir)
	node := NewNode(nodeID, "node.test", raftAddr, tmpDir)
	node.Start(context.Background())
	<-time.After(5 * time.Second)
	node.Stop()
}

func Test_ClusterConsensus(t *testing.T) {
	ctx := context.Background()
	nodeID := "node0"
	raftAddr := "127.0.0.1:0"
	tmpDir, _ := ioutil.TempDir("", nodeID)
	defer os.RemoveAll(tmpDir)
	node := NewNode(nodeID, "node.test", raftAddr, tmpDir)
	node.Start(ctx)
	defer node.Stop()

	<-time.After(5 * time.Second)
	nodeID2 := "node2"
	raftAddr2 := "127.0.0.1:2"
	tmpDir2, _ := ioutil.TempDir("", nodeID2)
	defer os.RemoveAll(tmpDir2)
	node2 := NewNode(nodeID2, "node.test", raftAddr2, tmpDir2)
	node2.Start(ctx)
	defer node2.Stop()

	// <-time.After(10 * time.Second)
	nodeID3 := "node3"
	raftAddr3 := "127.0.0.1:3"
	tmpDir3, _ := ioutil.TempDir("", nodeID3)
	defer os.RemoveAll(tmpDir3)
	node3 := NewNode(nodeID3, "node.test", raftAddr3, tmpDir3)
	node3.Start(ctx)
	defer node3.Stop()

	<-time.After(5 * time.Second)
	cf := node.raftStore.GetConfiguration()
	cf2 := node2.raftStore.GetConfiguration()
	cf3 := node3.raftStore.GetConfiguration()
	assert.Equal(t, cf, cf2)
	assert.Equal(t, cf2, cf3)
}

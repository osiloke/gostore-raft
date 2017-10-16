package cluster

import (
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

func TestNewNode(t *testing.T) {
	nodeID := "node0"
	raftAddr := "127.0.0.1:0"
	tmpDir, _ := ioutil.TempDir("", nodeID)
	defer os.RemoveAll(tmpDir)
	node := NewNode(nodeID, "node.test", raftAddr, tmpDir)
	node.run()
	<-time.After(5 * time.Second)
	node.Close()
}

func TestNodeConsensus(t *testing.T) {
	nodeID := "node0"
	raftAddr := "127.0.0.1:0"
	tmpDir, _ := ioutil.TempDir("", nodeID)
	defer os.RemoveAll(tmpDir)
	node := NewNode(nodeID, "node.test", raftAddr, tmpDir)
	go node.run()
	defer node.Close()
	<-time.After(5 * time.Second)
	nodeID2 := "node2"
	raftAddr2 := "127.0.0.1:2"
	tmpDir2, _ := ioutil.TempDir("", nodeID2)
	defer os.RemoveAll(tmpDir2)
	node2 := NewNode(nodeID2, "node.test", raftAddr2, tmpDir2)
	go node2.run()
	defer node2.Close()

	<-time.After(5 * time.Second)
	nodeID3 := "node3"
	raftAddr3 := "127.0.0.1:3"
	tmpDir3, _ := ioutil.TempDir("", nodeID3)
	defer os.RemoveAll(tmpDir3)
	node3 := NewNode(nodeID3, "node.test", raftAddr3, tmpDir3)
	go node3.run()
	defer node3.Close()

	<-time.After(5 * time.Second)
	cf := node.raftStore.GetConfiguration()
	log.Printf("node0 config %v", cf)
	cf2 := node2.raftStore.GetConfiguration()
	log.Printf("node2 config %v", cf2)
	cf3 := node3.raftStore.GetConfiguration()
	log.Printf("node3 config %v", cf3)
}

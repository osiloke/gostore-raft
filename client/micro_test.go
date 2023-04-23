package client

// import (
// 	"context"
// 	"fmt"
// 	"io/ioutil"
// 	"log"
// 	"os"
// 	"os/signal"
// 	"syscall"
// 	"testing"
// 	"time"

// 	"github.com/osiloke/gostore_raft/node"
// 	"github.com/stretchr/testify/assert"
// )

// type clusterNode struct {
// 	nd     *node.Node
// 	tmpDir string
// 	ID     string
// }

// func makeCluster(n int, ctx context.Context) []*clusterNode {
// 	cluster := []*clusterNode{}
// 	for i := 0; i < n; i++ {
// 		nodeID := fmt.Sprintf("node%d", i)
// 		tmpDir, _ := ioutil.TempDir("", nodeID)
// 		nd := node.NewExpectNode(nodeID, "test.nodes", fmt.Sprintf("127.0.0.1:500%d", i), tmpDir, n)
// 		cluster = append(cluster, &clusterNode{nd, tmpDir, nodeID})
// 		log.Printf("created %s", nodeID)
// 	}

// 	for _, v := range cluster {
// 		log.Printf("starting %s", v.ID)
// 		go v.nd.Start(ctx)
// 	}
// 	return cluster

// }
// func TestMicroClient_Get(t *testing.T) {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()
// 	cluster := makeCluster(3, ctx)
// 	defer func() {
// 		for _, v := range cluster {
// 			v.nd.Stop()
// 			os.RemoveAll(v.tmpDir)
// 		}
// 	}()
// 	ch := make(chan os.Signal, 1)
// 	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL)
// OUTER:
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			log.Println("context ended")
// 			break OUTER
// 		case <-ch:
// 			log.Println("received os signal")
// 			break OUTER
// 		case <-time.After(10 * time.Second):
// 			log.Println("timeout")
// 			break OUTER
// 		}
// 	}
// 	cl := NewMicroClient("test.nodes")

// 	_, err := cl.Get("nonexistent")
// 	if err != nil {
// 		log.Println("error getting key", err.Error())
// 	}
// 	assert.Nil(t, err)

// }

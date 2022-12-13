# gostore-raft

Raft implementation of gostore for horizontal scaling gostore stores

###

start a cluster

`./stored --raftAddr=127.0.0.1:5001 --nodeID=node1   `
`./stored --raftAddr=127.0.0.1:5001 --nodeID=node1   `
`./stored --raftAddr=127.0.0.1:5001 --nodeID=node1   `

call a store endpoint

```
 ~/go/bin/cli call go.micro.api.raft Store.Get '{"key": "hello"}'
```

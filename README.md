# gostore-raft

Raft implementation of gostore for horizontal scaling gostore stores

###

start a cluster

`./stored --raftAddr=127.0.0.1:5001 --nodeID=node1`
`./stored --raftAddr=127.0.0.1:5002 --nodeID=node2`
`./stored --raftAddr=127.0.0.1:5003 --nodeID=node3`

install go-micro

`go install github.com/go-micro/cli/cmd/go-micro@latest`

call a store endpoint

```
go-micro  call go.micro.raft Store.Get '{"key": "hello"}'
go-micro call go.micro.raft Store.Set '{"key": "hello", "val": "world"}'
```

protoc --proto_path=.  --go_out=. store.proto

package node

import (
	common "github.com/osiloke/gostore/common"
	"go-micro.dev/v4/server"
)

// Config stores node configuration
type Config struct {
	NodeID        string
	AdvertiseName string
	RaftAddr      string
	RaftDir       string
	Expect        int
	GoStore       common.ObjectStore
	Server        server.Server
	// other certificates for any inter-node communications. May not be set.
	NodeX509CACert string

	// NodeX509Cert is the path to the X509 cert for the Raft server. May not be set.
	NodeX509Cert string

	// NodeX509Key is the path to the X509 key for the Raft server. May not be set.
	NodeX509Key string

	// NoNodeVerify disables checking other nodes' Node X509 certs for validity.
	NoNodeVerify bool

	// NodeVerifyClient indicates whether a node should verify client certificates from
	// other nodes.
	NodeVerifyClient bool
}

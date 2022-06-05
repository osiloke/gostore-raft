package handler

import (
	"log"
	"strings"

	"go-micro.dev/v4/client"
	"go-micro.dev/v4/metadata"
	"go-micro.dev/v4/registry"
	"go-micro.dev/v4/selector"
	"golang.org/x/net/context"
)

// A Wrapper that creates a nodeid
type nodeSelector struct {
	client.Client
}

func (dc *nodeSelector) Call(ctx context.Context, req client.Request, rsp interface{}, opts ...client.CallOption) error {
	md, _ := metadata.FromContext(ctx)
	filter := func(services []*registry.Service) []*registry.Service {
		for _, service := range services {
			var nodes []*registry.Node
			for _, node := range service.Nodes {
				if strings.Contains(node.Id, md["NodeID"]) {
					nodes = append(nodes, node)
				}
			}
			service.Nodes = nodes
		}
		return services
	}

	callOptions := append(opts, client.WithSelectOption(
		selector.WithFilter(filter),
	))

	log.Printf("[Node Selector Wrapper] filtering for node %s\n", md["NodeID"])
	return dc.Client.Call(ctx, req, rsp, callOptions...)
}

func NewNodeSelectorWrapper(c client.Client) client.Client {
	return &nodeSelector{c}
}

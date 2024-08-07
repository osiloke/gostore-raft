// Code generated by protoc-gen-micro. DO NOT EDIT.
// source: service.proto

package service

import (
	fmt "fmt"
	proto "google.golang.org/protobuf/proto"
	math "math"
)

import (
	context "context"
	client "go-micro.dev/v5/client"
	server "go-micro.dev/v5/server"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ client.Option
var _ server.Option

// Client API for Service service

type Service interface {
	Join(ctx context.Context, in *Request, opts ...client.CallOption) (*Response, error)
	Remove(ctx context.Context, in *Request, opts ...client.CallOption) (*Response, error)
	Status(ctx context.Context, in *Request, opts ...client.CallOption) (*StatusResponse, error)
}

type service struct {
	c    client.Client
	name string
}

func NewService(name string, c client.Client) Service {
	return &service{
		c:    c,
		name: name,
	}
}

func (c *service) Join(ctx context.Context, in *Request, opts ...client.CallOption) (*Response, error) {
	req := c.c.NewRequest(c.name, "Service.Join", in)
	out := new(Response)
	err := c.c.Call(ctx, req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *service) Remove(ctx context.Context, in *Request, opts ...client.CallOption) (*Response, error) {
	req := c.c.NewRequest(c.name, "Service.Remove", in)
	out := new(Response)
	err := c.c.Call(ctx, req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *service) Status(ctx context.Context, in *Request, opts ...client.CallOption) (*StatusResponse, error) {
	req := c.c.NewRequest(c.name, "Service.Status", in)
	out := new(StatusResponse)
	err := c.c.Call(ctx, req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for Service service

type ServiceHandler interface {
	Join(context.Context, *Request, *Response) error
	Remove(context.Context, *Request, *Response) error
	Status(context.Context, *Request, *StatusResponse) error
}

func RegisterServiceHandler(s server.Server, hdlr ServiceHandler, opts ...server.HandlerOption) error {
	type service interface {
		Join(ctx context.Context, in *Request, out *Response) error
		Remove(ctx context.Context, in *Request, out *Response) error
		Status(ctx context.Context, in *Request, out *StatusResponse) error
	}
	type Service struct {
		service
	}
	h := &serviceHandler{hdlr}
	return s.Handle(s.NewHandler(&Service{h}, opts...))
}

type serviceHandler struct {
	ServiceHandler
}

func (h *serviceHandler) Join(ctx context.Context, in *Request, out *Response) error {
	return h.ServiceHandler.Join(ctx, in, out)
}

func (h *serviceHandler) Remove(ctx context.Context, in *Request, out *Response) error {
	return h.ServiceHandler.Remove(ctx, in, out)
}

func (h *serviceHandler) Status(ctx context.Context, in *Request, out *StatusResponse) error {
	return h.ServiceHandler.Status(ctx, in, out)
}

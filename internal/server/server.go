package server

import (
	"context"

	api "github.com/bachtg/raidkv/api/raidkv/v1"
	"github.com/bachtg/raidkv/internal/engine"
)

// Server = Engine + Grpc communication
type Server struct {
	api.UnimplementedKVServiceServer
	engine engine.Engine
}

func New(e engine.Engine) *Server {
	return &Server{engine: e}
}

func (s *Server) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	value, err := s.engine.Get(req.GetKey())
	if err != nil {
		return nil, toStatus(err)
	}

	return &api.GetResponse{
		Value: value,
	}, nil
}

func (s *Server) Put(ctx context.Context, req *api.PutRequest) (*api.PutResponse, error) {
	err := s.engine.Put(req.GetKey(), req.GetValue())
	if err != nil {
		return nil, toStatus(err)
	}

	return &api.PutResponse{}, nil
}

func (s *Server) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	err := s.engine.Delete(req.GetKey())
	if err != nil {
		return nil, toStatus(err)
	}

	return &api.DeleteResponse{}, nil
}

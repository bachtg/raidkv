package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	api "github.com/bachtg/raidkv/api/raidkv/v1"
	"github.com/bachtg/raidkv/internal/engine"
)

type replicationServer struct {
	api.UnimplementedReplicationServiceServer
	engine engine.Engine
}

func (s *replicationServer) ReadWAL(ctx context.Context, req *api.ReadWALRequest) (*api.ReadWALResponse, error) {
	r, ok := s.engine.(engine.WALReader)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "engine has no wal to read")
	}

	recs, err := r.ReadFrom(req.GetFrom(), int(req.GetLimit()))
	if err != nil {
		return nil, toStatus(err)
	}

	out := make([]*api.WALRecord, len(recs))
	for i, rec := range recs {
		out[i] = &api.WALRecord{Seq: rec.Seq, Delete: rec.Delete, Key: rec.Key, Value: rec.Value}
	}
	return &api.ReadWALResponse{Records: out}, nil
}

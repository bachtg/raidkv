package server

import (
	"fmt"

	"buf.build/go/protovalidate"
	pvinterceptor "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"
	"google.golang.org/grpc"

	api "github.com/bachtg/raidkv/api/raidkv/v1"
	"github.com/bachtg/raidkv/internal/engine"
)

func NewGRPCServer(e engine.Engine) (*grpc.Server, error) {
	validator, err := protovalidate.New()
	if err != nil {
		return nil, fmt.Errorf("create validator: %w", err)
	}

	g := grpc.NewServer(
		grpc.UnaryInterceptor(pvinterceptor.UnaryServerInterceptor(validator)),
	)

	api.RegisterKVServiceServer(g, New(e))

	return g, nil
}

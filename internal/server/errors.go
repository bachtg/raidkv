package server

import (
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/bachtg/raidkv/internal/engine"
)

func toStatus(err error) error {
	switch {
	case errors.Is(err, engine.ErrNotFound):
		return status.Error(codes.NotFound, "key not found")
	default:
		slog.Error("engine error", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
}

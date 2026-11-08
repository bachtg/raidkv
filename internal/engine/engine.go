package engine

import (
	"errors"
	"fmt"
)

// ErrNotFound is returned by Get when the key does not exist.
// It is a storage-level error; translating it to a gRPC status is the
// server's job (internal/server), so the engine never imports gRPC.
var ErrNotFound = errors.New("engine: key not found")

// ErrClosed is returned by Put and Delete after Close.
var ErrClosed = errors.New("engine: closed")

// ErrInvalidArgument is returned by Put and Delete when a key or value is
// longer than the engine accepts. Rejecting it up front keeps the WAL free of
// records that replay would treat as corrupt.
var ErrInvalidArgument = errors.New("engine: invalid argument")

// MaxKeyLength must match max_len on the key fields in raidkv.proto.
const (
	MaxKeyLength   = 10000
	MaxValueLength = 4 << 20 // 4 MiB
)

func validate(key, value []byte) error {
	if len(key) > MaxKeyLength {
		return fmt.Errorf("%w: key length %d exceeds limit %d", ErrInvalidArgument, len(key), MaxKeyLength)
	}
	if len(value) > MaxValueLength {
		return fmt.Errorf("%w: value length %d exceeds limit %d", ErrInvalidArgument, len(value), MaxValueLength)
	}
	return nil
}

type Engine interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	Close() error
}

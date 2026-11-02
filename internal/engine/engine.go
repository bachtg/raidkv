package engine

import "errors"

// ErrNotFound is returned by Get when the key does not exist.
// It is a storage-level error; translating it to a gRPC status is the
// server's job (internal/server), so the engine never imports gRPC.
var ErrNotFound = errors.New("engine: key not found")

type Engine interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	Close() error
}

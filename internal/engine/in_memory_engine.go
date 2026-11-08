package engine

import (
	"bytes"
	"sync"
)

type memEngine struct {
	storage map[string][]byte
	mu      sync.RWMutex
}

func NewMemEngine() Engine {
	return &memEngine{
		storage: make(map[string][]byte),
	}
}

func (e *memEngine) Get(key []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	value, ok := e.storage[string(key)]
	if !ok {
		return nil, ErrNotFound
	}

	return bytes.Clone(value), nil
}

func (e *memEngine) Put(key, value []byte) error {
	if err := validate(key, value); err != nil {
		return err
	}

	clonedValue := bytes.Clone(value)

	e.mu.Lock()
	defer e.mu.Unlock()

	e.storage[string(key)] = clonedValue

	return nil
}

func (e *memEngine) Delete(key []byte) error {
	if err := validate(key, nil); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.storage, string(key))

	return nil
}

func (e *memEngine) Close() error {
	return nil
}

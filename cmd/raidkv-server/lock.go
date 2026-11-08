package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockDataDir takes an exclusive lock on dir/LOCK so that two servers never
// append to the same WAL. The kernel drops the lock when the process exits,
// including on kill -9, so a crash never leaves a stale lock behind.
func lockDataDir(dir string) (unlock func() error, err error) {
	f, err := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("data dir %s is in use by another process", dir)
		}
		return nil, fmt.Errorf("lock data dir %s: %w", dir, err)
	}
	return f.Close, nil
}

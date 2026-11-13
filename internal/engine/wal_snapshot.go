package engine

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

const snapshotTempFileName = "snapshot.tmp.log"

// maybeSnapshot starts a snapshot once the wal outgrows the last one. Hold e.mu.
func (e *walEngine) maybeSnapshot() {
	trigger := max(e.snapshotThreshold, e.snapshotSize)
	if e.walSize >= trigger && !e.snapshotting {
		e.snapshotting = true
		go e.snapshot()
	}
}

func (e *walEngine) snapshot() (err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	defer func() {
		e.snapshotting = false
		if err != nil {
			// walSize is only reset on success, so the next write retries.
			slog.Error("wal: snapshot failed, will retry on a later write", "dir", e.dir, "err", err)
		}
	}()

	if e.closed {
		return nil
	}

	path := filepath.Join(e.dir, snapshotTempFileName)

	// O_CREATE: create new file if not exist
	// O_RDWR: open for read and write
	// O_TRUNC: empty a tmp file left behind by an earlier failed snapshot
	// 0644: read/write permission
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	defer file.Close()

	// load data from map to snapshot file
	dataSize := int64(0)

	for key, value := range e.storage {
		encodedRecord := encodeRecord(operationPut, []byte(key), value)
		dataSize += int64(len(encodedRecord))
		_, err = file.Write(encodedRecord)
		if err != nil {
			return err
		}
	}

	temp := make([]byte, 0, 8)
	_, err = file.Write(encodeRecord(operationEnd, nil, binary.BigEndian.AppendUint64(temp, uint64(dataSize))))
	if err != nil {
		return err
	}

	// fsync file snapshot after loading
	if err := file.Sync(); err != nil {
		return err
	}

	err = os.Rename(path, filepath.Join(e.dir, snapshotFileName))
	if err != nil {
		return err
	}

	if err = SyncDir(e.dir); err != nil {
		return fmt.Errorf("wal: %w", err)
	}

	err = e.file.Truncate(0)
	if err != nil {
		return err
	}

	if err := e.file.Sync(); err != nil {
		panic(fmt.Sprintf("wal: fsync after truncate failed, cannot tell what is on disk: %v", err))
	}

	e.walSize = 0
	e.snapshotSize = dataSize

	return nil
}

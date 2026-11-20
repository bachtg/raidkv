package engine

import (
	"bytes"
	"fmt"
)

func (e *walEngine) append(op byte, key, value []byte) error {
	if err := validate(key, value); err != nil {
		return err
	}

	clonedValue := bytes.Clone(value)

	e.mu.Lock()
	defer e.mu.Unlock()

	for e.snapshotting {
		e.syncCond.Wait()
	}

	if e.closed {
		return ErrClosed
	}

	e.sequenceNumber++
	encodedRecord := encodeRecord(e.sequenceNumber, op, key, value)

	_, err := e.file.Write(encodedRecord)
	if err != nil {
		panic(fmt.Sprintf("wal: write failed, log may hold a partial record: %v", err))
	}

	e.written += int64(len(encodedRecord))
	currentOffset := e.written

	e.walSize += int64(len(encodedRecord))

	switch e.syncMode {
	case SyncPeriodic:
		e.apply(op, string(key), clonedValue)
	case SyncGroup:
		e.pendingRecords = append(e.pendingRecords, pendingRecord{
			op:     op,
			key:    string(key),
			value:  clonedValue,
			offset: currentOffset,
		})

		e.waitDurable(currentOffset)
	case SyncAlways:
		if err := e.file.Sync(); err != nil {
			panic(fmt.Sprintf("wal: fsync failed, cannot guarantee durability: %v", err))
		}
		e.syncedSeq = e.sequenceNumber
		e.apply(op, string(key), clonedValue)
	}

	e.maybeSnapshot()

	return nil
}

// apply sets or deletes key in the map. Hold e.mu.
func (e *walEngine) apply(op byte, key string, value []byte) {
	if op == operationPut {
		e.storage[key] = value
		return
	}
	delete(e.storage, key)
}

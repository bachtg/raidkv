package engine

import (
	"bytes"
	"errors"
	"fmt"
)

// ErrSeqGap is returned by Apply when a record skips past the next seq.
var ErrSeqGap = errors.New("engine: seq gap")

// Apply writes a record from the primary with its seq. Duplicates are skipped.
func (e *walEngine) Apply(rec Record) error {
	var (
		op    = operationPut
		key   = rec.Key
		value = rec.Value
	)

	if rec.Delete {
		op = operationDel
	}

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

	if rec.Seq <= e.sequenceNumber {
		return nil
	}

	if rec.Seq > e.sequenceNumber+1 {
		return fmt.Errorf("%w: got seq %d, want %d", ErrSeqGap, rec.Seq, e.sequenceNumber+1)
	}

	e.sequenceNumber = rec.Seq
	encodedRecord := encodeRecord(rec.Seq, op, key, value)

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

// LastSeq returns the highest seq written, fsynced or not.
func (e *walEngine) LastSeq() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.sequenceNumber
}

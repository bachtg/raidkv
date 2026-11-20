package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// ErrTooOld is returned by ReadFrom when the records asked for were cut from
// the wal by a snapshot. The reader needs the snapshot first.
var ErrTooOld = errors.New("engine: records no longer in the wal")

// Record is one Put or Delete read back from the wal.
type Record struct {
	Seq    int64
	Delete bool
	Key    []byte
	Value  []byte // empty for a Delete
}

// ReadFrom returns up to limit fsynced records with seq >= from, in seq
// order. It returns no records if there is nothing new yet, and ErrTooOld if
// from is already covered by the snapshot.
func (e *walEngine) ReadFrom(from int64, limit int) ([]Record, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("%w: limit must be positive, got %d", ErrInvalidArgument, limit)
	}

	file, err := os.OpenFile(filepath.Join(e.dir, walFileName), os.O_RDONLY, 0o644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	walGen := int64(0)
	var records []Record

	for {
		e.mu.RLock()
		walGen = e.walGeneration
		var (
			snapshotSeq = e.snapshotSeq
			syncedSeq   = e.syncedSeq
		)

		e.mu.RUnlock()

		if from <= snapshotSeq {
			return nil, ErrTooOld
		}

		if from > syncedSeq {
			return nil, nil
		}

		_, err := file.Seek(0, io.SeekStart)
		if err != nil {
			return nil, err
		}

		records, err = readOnce(file, from, syncedSeq, limit)

		e.mu.RLock()
		changed := walGen != e.walGeneration
		e.mu.RUnlock()

		if changed {
			continue
		}

		if err == nil && len(records) > 0 && records[0].Seq != from {
			return nil, fmt.Errorf("%w: read from seq %d, got seq %d first", ErrCorruptWAL, from, records[0].Seq)
		}

		return records, err
	}
}

func readOnce(reader io.Reader, from, syncedSeq int64, limit int) ([]Record, error) {
	records := make([]Record, 0)
	offset := int64(0)
	prevSeq := int64(0)

	for {
		var (
			checksum     = make([]byte, 4)
			op           = make([]byte, 1)
			seqNumber    = make([]byte, 8)
			leng         = make([]byte, 4)
			fullRecord   = make([]byte, 0)
			recordLength = 0
		)

		// read first 4 bytes to extract record checksum
		if _, err := io.ReadFull(reader, checksum); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return records, nil
			}
			return nil, err
		}
		recordLength += 4

		// read next 8 bytes to extract record sequence number
		if _, err := io.ReadFull(reader, seqNumber); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return records, nil
			}
			return nil, err
		}
		recordLength += 8
		fullRecord = append(fullRecord, seqNumber...)

		// read next byte to extract operation
		if _, err := io.ReadFull(reader, op); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return records, nil
			}
			return nil, err
		}
		fullRecord = append(fullRecord, op...)

		recordLength++

		// read next 4 bytes to extract key length
		if _, err := io.ReadFull(reader, leng); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return records, nil
			}
			return nil, err
		}

		recordLength += 4

		keyLen := binary.BigEndian.Uint32(leng)

		if keyLen > MaxKeyLength {
			return nil, fmt.Errorf("%w at offset %d: key length %d exceeds limit %d", ErrCorruptWAL, offset, keyLen, MaxKeyLength)
		}
		fullRecord = append(fullRecord, leng...)
		key := make([]byte, keyLen)

		// read next keyLen bytes to extract key
		if _, err := io.ReadFull(reader, key); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return records, nil
			}
			return nil, err
		}
		fullRecord = append(fullRecord, key...)

		recordLength += int(keyLen)

		// read next 4 bytes to extract value length
		if _, err := io.ReadFull(reader, leng); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return records, nil
			}
			return nil, err
		}

		recordLength += 4

		valueLen := binary.BigEndian.Uint32(leng)

		if valueLen > MaxValueLength {
			return nil, fmt.Errorf("%w at offset %d: value length %d exceeds limit %d", ErrCorruptWAL, offset, valueLen, MaxValueLength)
		}

		fullRecord = append(fullRecord, leng...)

		value := make([]byte, valueLen)

		// read next valueLen bytes to extract value
		if _, err := io.ReadFull(reader, value); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return records, nil
			}
			return nil, err
		}

		fullRecord = append(fullRecord, value...)

		recordLength += int(valueLen)

		storedChecksum := binary.BigEndian.Uint32(checksum)
		readChecksum := crc32.Checksum(fullRecord, crcTable)

		if storedChecksum != readChecksum {
			return nil, fmt.Errorf("%w at offset %d: checksum mismatch (stored %#08x, computed %#08x)", ErrCorruptWAL, offset, storedChecksum, readChecksum)
		}

		seq := int64(binary.BigEndian.Uint64(seqNumber))

		if prevSeq == 0 {
			prevSeq = seq
		} else if prevSeq+1 != seq {
			return nil, fmt.Errorf("%w at offset %d: seq %d after %d", ErrCorruptWAL, offset, seq, prevSeq)
		} else {
			prevSeq = seq
		}

		if seq > syncedSeq {
			break
		}

		if seq >= from {
			records = append(records, Record{
				Seq:    seq,
				Key:    key,
				Value:  value,
				Delete: op[0] == operationDel,
			})
		}

		if len(records) >= limit {
			break
		}

		offset += int64(recordLength)
	}

	return records, nil
}

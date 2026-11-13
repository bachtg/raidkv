package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	operationPut byte = 1
	operationDel byte = 2
	operationEnd byte = 3
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

var ErrCorruptWAL = errors.New("wal: corrupt record")

// wal = write-ahead log
// principle: before applying any changes to the main data store, write the changes to append-only log.
// data format: [checksum][operation][key_len][key][value_len][value]
type walEngine struct {
	dir     string
	file    *os.File
	storage map[string][]byte
	mu      sync.RWMutex
	closed  bool
	walSize int64

	snapshotThreshold int64
	snapshotSize      int64
	snapshotting      bool
}

const (
	walFileName      = "wal.log"
	snapshotFileName = "snapshot.log"
)

// NewWALEngine opens the engine in dir and snapshots once wal.log reaches
// snapshotThreshold bytes.
func NewWALEngine(dir string, snapshotThreshold int64) (Engine, error) {
	if snapshotThreshold <= 0 {
		return nil, fmt.Errorf("%w: snapshot threshold must be positive, got %d", ErrInvalidArgument, snapshotThreshold)
	}

	engine := &walEngine{
		dir:               dir,
		storage:           make(map[string][]byte),
		snapshotThreshold: snapshotThreshold,
	}

	// Left by a snapshot that never reached the rename.
	err := os.Remove(filepath.Join(dir, snapshotTempFileName))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	err = engine.loadDataFromFile(snapshotFileName)
	if err != nil {
		return nil, err
	}

	err = engine.loadDataFromFile(walFileName)
	if err != nil {
		return nil, err
	}

	return engine, nil
}

func (e *walEngine) loadDataFromFile(fileName string) error {
	path := filepath.Join(e.dir, fileName)
	isSnapshot := fileName == snapshotFileName

	// O_CREATE: create new file if not exist
	// O_RDWR: open for read and write
	// 0644: read/write permission
	flags := os.O_CREATE | os.O_RDWR | os.O_APPEND
	if isSnapshot {
		// Only snapshot() creates it: missing is fine, empty is damage.
		flags = os.O_RDONLY
	}
	file, err := os.OpenFile(path, flags, 0o644)
	if isSnapshot && errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}

	// The file may have just been created; make its name durable too.
	if err = SyncDir(e.dir); err != nil {
		file.Close()
		return fmt.Errorf("wal: %w", err)
	}

	fileStat, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}

	offset, err := replayRecords(e.storage, file, fileStat.Size(), isSnapshot)
	if err != nil {
		file.Close()
		return fmt.Errorf("%s: %w", fileName, err)
	}

	if isSnapshot {
		e.snapshotSize = offset
		return file.Close()
	}

	e.file = file
	e.walSize = offset

	err = file.Truncate(offset)
	if err != nil {
		file.Close()
		return err
	}

	return nil
}

func replayRecords(store map[string][]byte, reader io.Reader, fileSize int64, snapshot bool) (int64, error) {
	offset := int64(0)

	// A torn wal tail was never acked, so drop it; a short snapshot is damage.
	cut := func() (int64, error) {
		if snapshot {
			return 0, fmt.Errorf("%w at offset %d: snapshot ends without its end record", ErrCorruptWAL, offset)
		}
		return offset, nil
	}

	for {
		var (
			checksum     = make([]byte, 4)
			op           = make([]byte, 1)
			leng         = make([]byte, 4)
			fullRecord   = make([]byte, 0)
			recordLength = 0
		)

		// read first 4 bytes to extract record checksum
		if _, err := io.ReadFull(reader, checksum); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return cut()
			}
			return 0, err
		}
		recordLength += 4

		// read next byte to extract operation
		if _, err := io.ReadFull(reader, op); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return cut()
			}
			return 0, err
		}
		fullRecord = append(fullRecord, op...)

		recordLength++

		// read next 4 bytes to extract key length
		if _, err := io.ReadFull(reader, leng); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return cut()
			}
			return 0, err
		}

		recordLength += 4

		keyLen := binary.BigEndian.Uint32(leng)

		if keyLen > MaxKeyLength {
			return 0, fmt.Errorf("%w at offset %d: key length %d exceeds limit %d", ErrCorruptWAL, offset, keyLen, MaxKeyLength)
		}
		fullRecord = append(fullRecord, leng...)
		key := make([]byte, keyLen)

		// read next keyLen bytes to extract key
		if _, err := io.ReadFull(reader, key); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return cut()
			}
			return 0, err
		}
		fullRecord = append(fullRecord, key...)

		recordLength += int(keyLen)

		// read next 4 bytes to extract value length
		if _, err := io.ReadFull(reader, leng); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return cut()
			}
			return 0, err
		}

		recordLength += 4

		valueLen := binary.BigEndian.Uint32(leng)

		if valueLen > MaxValueLength {
			return 0, fmt.Errorf("%w at offset %d: value length %d exceeds limit %d", ErrCorruptWAL, offset, valueLen, MaxValueLength)
		}

		fullRecord = append(fullRecord, leng...)

		value := make([]byte, valueLen)

		// read next valueLen bytes to extract value
		if _, err := io.ReadFull(reader, value); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return cut()
			}
			return 0, err
		}

		fullRecord = append(fullRecord, value...)

		recordLength += int(valueLen)

		storedChecksum := binary.BigEndian.Uint32(checksum)
		readChecksum := crc32.Checksum(fullRecord, crcTable)

		if storedChecksum != readChecksum {
			if snapshot || offset+int64(recordLength) < fileSize {
				return 0, fmt.Errorf("%w at offset %d: checksum mismatch (stored %#08x, computed %#08x)", ErrCorruptWAL, offset, storedChecksum, readChecksum)
			}
			return offset, nil
		}

		switch op[0] {
		case operationPut:
			store[string(key)] = value
		case operationDel:
			delete(store, string(key))
		case operationEnd:
			if !snapshot {
				return 0, fmt.Errorf("%w at offset %d: unknown operation %#02x", ErrCorruptWAL, offset, op[0])
			}

			if valueLen != 8 {
				return 0, fmt.Errorf("%w at offset %d: end record holds %d bytes, want 8", ErrCorruptWAL, offset, valueLen)
			}
			if size := binary.BigEndian.Uint64(value); size != uint64(offset) {
				return 0, fmt.Errorf("%w at offset %d: end record says the data is %d bytes", ErrCorruptWAL, offset, size)
			}
			if end := offset + int64(recordLength); end != fileSize {
				return 0, fmt.Errorf("%w at offset %d: %d bytes after the end record", ErrCorruptWAL, offset, fileSize-end)
			}
			return offset, nil
		default:
			return 0, fmt.Errorf("%w at offset %d: unknown operation %#02x", ErrCorruptWAL, offset, op[0])
		}

		offset += int64(recordLength)
	}
}

func (e *walEngine) Get(key []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	value, ok := e.storage[string(key)]
	if !ok {
		return nil, ErrNotFound
	}

	return bytes.Clone(value), nil
}

// append log into file
func (e *walEngine) Put(key, value []byte) error {
	if err := validate(key, value); err != nil {
		return err
	}

	var (
		encodedRecord = encodeRecord(operationPut, key, value)
		clonedValue   = bytes.Clone(value)
	)

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}

	_, err := e.file.Write(encodedRecord)
	if err != nil {
		panic(fmt.Sprintf("wal: write failed, log may hold a partial record: %v", err))
	}

	if err := e.file.Sync(); err != nil {
		panic(fmt.Sprintf("wal: fsync failed, cannot guarantee durability: %v", err))
	}

	e.storage[string(key)] = clonedValue

	e.walSize += int64(len(encodedRecord))

	e.maybeSnapshot()

	return nil
}

func encodeRecord(op byte, key, value []byte) []byte {
	var (
		keyLen   = uint32(len(key))
		valueLen = uint32(len(value))
		logBytes = make([]byte, 0, 13+len(key)+len(value))
	)

	logBytes = append(logBytes, 0, 0, 0, 0)

	logBytes = append(logBytes, op)

	logBytes = binary.BigEndian.AppendUint32(logBytes, keyLen)
	logBytes = append(logBytes, key...)

	logBytes = binary.BigEndian.AppendUint32(logBytes, valueLen)
	logBytes = append(logBytes, value...)

	checksum := crc32.Checksum(logBytes[4:], crcTable)

	binary.BigEndian.PutUint32(logBytes, checksum)

	return logBytes
}

func (e *walEngine) Delete(key []byte) error {
	if err := validate(key, nil); err != nil {
		return err
	}

	encodedRecord := encodeRecord(operationDel, key, nil)

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}

	_, err := e.file.Write(encodedRecord)
	if err != nil {
		panic(fmt.Sprintf("wal: write failed, log may hold a partial record: %v", err))
	}

	if err := e.file.Sync(); err != nil {
		panic(fmt.Sprintf("wal: fsync failed, cannot guarantee durability: %v", err))
	}

	delete(e.storage, string(key))

	e.walSize += int64(len(encodedRecord))

	e.maybeSnapshot()

	return nil
}

// Close waits for any Put or Delete in progress, so a request still running
// during shutdown gets ErrClosed instead of writing to a closed file.
func (e *walEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	return e.file.Close()
}

package engine_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bachtg/raidkv/internal/engine"
	"github.com/bachtg/raidkv/internal/engine/enginetest"
)

func TestWALEngine(t *testing.T) {
	enginetest.Run(t, func(t *testing.T) engine.Engine {
		return openWAL(t, filepath.Join(t.TempDir(), "wal.log"))
	})
}

func TestWALReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	e := openWAL(t, path)
	put(t, e, "a", "1")
	put(t, e, "b", "2")
	put(t, e, "gone", "x")
	if err := e.Delete([]byte("gone")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	closeWAL(t, e)

	e = openWAL(t, path)
	put(t, e, "c", "3")
	closeWAL(t, e)

	e = openWAL(t, path)
	defer closeWAL(t, e)
	wantValue(t, e, "a", "1")
	wantValue(t, e, "b", "2")
	wantValue(t, e, "c", "3")
	wantMissing(t, e, "gone")
}

func TestWALWriteAfterClose(t *testing.T) {
	e := openWAL(t, filepath.Join(t.TempDir(), "wal.log"))
	put(t, e, "a", "1")
	closeWAL(t, e)

	if err := e.Put([]byte("b"), []byte("2")); !errors.Is(err, engine.ErrClosed) {
		t.Fatalf("Put after Close: %v, want ErrClosed", err)
	}
	if err := e.Delete([]byte("a")); !errors.Is(err, engine.ErrClosed) {
		t.Fatalf("Delete after Close: %v, want ErrClosed", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// Every possible cut of the last record, as a crash in the middle of Write
// would leave it. The cut record must be dropped, and the file must be
// truncated so that later appends are readable.
func TestWALTornLastRecord(t *testing.T) {
	_, bStart, size := writeAB(t)
	for cut := int64(1); cut <= size-bStart; cut++ {
		path, _, _ := writeAB(t)
		if err := os.Truncate(path, size-cut); err != nil {
			t.Fatal(err)
		}

		e, err := engine.NewWalEngine(path)
		if err != nil {
			t.Fatalf("cut %d: reopen: %v", cut, err)
		}
		wantValue(t, e, "a", "1")
		wantMissing(t, e, "b")
		put(t, e, "c", "3")
		closeWAL(t, e)

		e, err = engine.NewWalEngine(path)
		if err != nil {
			t.Fatalf("cut %d: reopen after append: %v", cut, err)
		}
		wantValue(t, e, "a", "1")
		wantMissing(t, e, "b")
		wantValue(t, e, "c", "3")
		closeWAL(t, e)
	}
}

// Record layout: [crc 4][op 1][key_len 4][key][value_len 4][value].
func TestWALCorruption(t *testing.T) {
	const (
		crcOff      = 0
		opOff       = 4
		keyLenOff   = 5
		keyOff      = 9
		valueOffKey = 9 + 1 + 4 // value of a 1-byte key
	)

	tests := []struct {
		name     string
		offset   func(bStart int64) int64
		value    byte
		wantFail bool
	}{
		{"middle record value", func(int64) int64 { return valueOffKey }, '9', true},
		{"middle record key", func(int64) int64 { return keyOff }, 'z', true},
		{"middle record crc", func(int64) int64 { return crcOff }, 0xff, true},
		{"middle record key_len", func(int64) int64 { return keyLenOff }, 0x7f, true},
		{"last record op", func(b int64) int64 { return b + opOff }, 0x09, false},
		{"last record value", func(b int64) int64 { return b + valueOffKey }, '9', false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, bStart, _ := writeAB(t)
			flipByte(t, path, tt.offset(bStart), tt.value)

			e, err := engine.NewWalEngine(path)
			if tt.wantFail {
				if !errors.Is(err, engine.ErrCorruptWAL) {
					t.Fatalf("NewWalEngine err = %v, want ErrCorruptWAL", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewWalEngine: %v (a corrupt last record must be dropped, not fail)", err)
			}
			wantValue(t, e, "a", "1")
			wantMissing(t, e, "b")
			put(t, e, "c", "3")
			closeWAL(t, e)

			e = openWAL(t, path)
			defer closeWAL(t, e)
			wantValue(t, e, "a", "1")
			wantMissing(t, e, "b")
			wantValue(t, e, "c", "3")
		})
	}
}

// writeAB writes Put(a,1) then Put(b,xyz) and returns the file path, the
// offset where record b starts, and the file size.
func writeAB(t *testing.T) (path string, bStart, size int64) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "wal.log")
	e := openWAL(t, path)
	put(t, e, "a", "1")
	bStart = fileSize(t, path)
	put(t, e, "b", "xyz")
	closeWAL(t, e)
	return path, bStart, fileSize(t, path)
}

func openWAL(t *testing.T, path string) engine.Engine {
	t.Helper()
	e, err := engine.NewWalEngine(path)
	if err != nil {
		t.Fatalf("NewWalEngine: %v", err)
	}
	return e
}

func closeWAL(t *testing.T, e engine.Engine) {
	t.Helper()
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func put(t *testing.T, e engine.Engine, k, v string) {
	t.Helper()
	if err := e.Put([]byte(k), []byte(v)); err != nil {
		t.Fatalf("Put(%q): %v", k, err)
	}
}

func wantValue(t *testing.T, e engine.Engine, k, want string) {
	t.Helper()
	got, err := e.Get([]byte(k))
	if err != nil || string(got) != want {
		t.Fatalf("Get(%q) = %q, %v; want %q", k, got, err, want)
	}
}

func wantMissing(t *testing.T, e engine.Engine, k string) {
	t.Helper()
	got, err := e.Get([]byte(k))
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("Get(%q) = %q, %v; want ErrNotFound", k, got, err)
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func flipByte(t *testing.T, path string, offset int64, value byte) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if data[offset] == value {
		t.Fatalf("byte at %d is already %#x; the test would not change anything", offset, value)
	}
	data[offset] = value
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

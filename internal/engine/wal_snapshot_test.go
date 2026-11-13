package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// noSnapshot keeps the trigger out of the way; these tests call snapshot().
const noSnapshot = 1 << 40

func TestSnapshotThenReopen(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir)
	mustPut(t, e, "a", "1")
	mustPut(t, e, "b", "2")
	mustPut(t, e, "c", "3") // only in the snapshot
	mustPut(t, e, "gone", "x")
	if err := e.Delete([]byte("gone")); err != nil {
		t.Fatal(err)
	}

	mustSnapshot(t, e)
	if e.walSize != 0 || sizeOf(t, filepath.Join(dir, walFileName)) != 0 {
		t.Fatalf("after snapshot: walSize = %d, wal.log = %d bytes; want 0, 0",
			e.walSize, sizeOf(t, filepath.Join(dir, walFileName)))
	}

	// Writes after the snapshot live only in the wal.
	mustPut(t, e, "a", "10")
	if err := e.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	e.Close()

	e = openEngine(t, dir)
	defer e.Close()
	wantStore(t, e, map[string]string{"a": "10", "c": "3"})
}

// A failed snapshot leaves its tmp file; the next one must not append to it.
func TestSnapshotOverwritesLeftoverTmp(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir)
	mustPut(t, e, "a", "1")

	zombie := encodeRecord(operationPut, []byte("zombie"), []byte("z"))
	if err := os.WriteFile(filepath.Join(dir, snapshotTempFileName), zombie, 0o644); err != nil {
		t.Fatal(err)
	}

	mustSnapshot(t, e)
	e.Close()

	e = openEngine(t, dir)
	defer e.Close()
	wantStore(t, e, map[string]string{"a": "1"})
}

func TestOpenRemovesLeftoverTmp(t *testing.T) {
	dir := t.TempDir()
	tmp := filepath.Join(dir, snapshotTempFileName)
	if err := os.WriteFile(tmp, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	e := openEngine(t, dir)
	defer e.Close()
	if _, err := os.Stat(tmp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat %s: %v, want not exist", snapshotTempFileName, err)
	}
}

// A damaged snapshot means lost acked writes: fail, never trim.
func TestCorruptSnapshotFailsOpen(t *testing.T) {
	// Snapshot of a=1, b=2: two 15-byte records, then the end record.
	const recLen = 15
	endRecLen := len(encodeRecord(operationEnd, nil, make([]byte, 8)))

	tests := []struct {
		name   string
		damage func(data []byte) []byte
	}{
		{"cut inside the end record", func(d []byte) []byte { return d[:len(d)-1] }},
		{"bad byte in the end record", func(d []byte) []byte { d[len(d)-1] ^= 0xff; return d }},
		{"bad byte in the first record", func(d []byte) []byte { d[9] ^= 0xff; return d }}, // its key
		{"end record cut off", func(d []byte) []byte { return d[:len(d)-endRecLen] }},
		{"end record and last record cut off", func(d []byte) []byte { return d[:len(d)-endRecLen-recLen] }},
		{"first record missing", func(d []byte) []byte { return d[recLen:] }},
		{"record after the end record", func(d []byte) []byte {
			return append(d, encodeRecord(operationPut, []byte("zombie"), []byte("z"))...)
		}},
		{"end record with a short value", func(d []byte) []byte {
			return append(d[:len(d)-endRecLen], encodeRecord(operationEnd, nil, []byte{0, 30})...)
		}},
		{"empty file", func([]byte) []byte { return nil }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			e := openEngine(t, dir)
			mustPut(t, e, "a", "1")
			mustPut(t, e, "b", "2")
			mustSnapshot(t, e)
			e.Close()

			path := filepath.Join(dir, snapshotFileName)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, tt.damage(data), 0o644); err != nil {
				t.Fatal(err)
			}
			sizeBefore := sizeOf(t, path)

			if _, err := NewWALEngine(dir, noSnapshot); !errors.Is(err, ErrCorruptWAL) {
				t.Fatalf("NewWALEngine err = %v, want ErrCorruptWAL", err)
			}
			if got := sizeOf(t, path); got != sizeBefore {
				t.Fatalf("snapshot.log was trimmed from %d to %d bytes", sizeBefore, got)
			}
		})
	}
}

func TestOpenWithoutSnapshot(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir)
	mustPut(t, e, "a", "1")
	e.Close()

	e = openEngine(t, dir)
	defer e.Close()
	wantStore(t, e, map[string]string{"a": "1"})
	if _, err := os.Stat(filepath.Join(dir, snapshotFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat %s: %v, want not exist", snapshotFileName, err)
	}
}

// The end record belongs to snapshots only.
func TestEndRecordInWALFailsOpen(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir)
	mustPut(t, e, "a", "1")
	e.Close()

	wal := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(wal, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Well-formed, so only its place is wrong.
	end := encodeRecord(operationEnd, nil, binary.BigEndian.AppendUint64(nil, uint64(sizeOf(t, wal))))
	if _, err := f.Write(end); err != nil {
		t.Fatal(err)
	}
	f.Close()

	if _, err := NewWALEngine(dir, noSnapshot); !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("NewWALEngine err = %v, want ErrCorruptWAL", err)
	}
}

func TestSnapshotAfterCloseDoesNothing(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir)
	mustPut(t, e, "a", "1")
	e.Close()

	if err := e.snapshot(); err != nil {
		t.Fatalf("snapshot after Close: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, snapshotFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat %s after Close: %v, want not exist", snapshotFileName, err)
	}
}

// walSize drives the trigger, so it must always match the file size.
func TestWALSizeMatchesFile(t *testing.T) {
	dir := t.TempDir()
	wal := filepath.Join(dir, walFileName)

	e := openEngine(t, dir)
	mustPut(t, e, "a", "1")
	mustPut(t, e, "b", "xyz")
	e.Close()
	if err := os.Truncate(wal, sizeOf(t, wal)-2); err != nil {
		t.Fatal(err)
	}

	e = openEngine(t, dir)
	defer e.Close()
	check := func(when string) {
		t.Helper()
		if got := sizeOf(t, wal); e.walSize != got {
			t.Fatalf("%s: walSize = %d, wal.log = %d bytes", when, e.walSize, got)
		}
	}
	check("after dropping the torn tail")
	mustPut(t, e, "c", "3")
	if err := e.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	check("after writes")
	mustSnapshot(t, e)
	check("after snapshot")
}

func TestSnapshotSizeSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir)
	if e.snapshotSize != 0 {
		t.Fatalf("no snapshot yet: snapshotSize = %d, want 0", e.snapshotSize)
	}
	mustPut(t, e, "a", "1")
	mustPut(t, e, "b", "2")
	mustSnapshot(t, e)
	afterSnapshot := e.snapshotSize
	e.Close()

	e = openEngine(t, dir)
	defer e.Close()
	if afterSnapshot == 0 || e.snapshotSize != afterSnapshot {
		t.Fatalf("snapshotSize = %d after snapshot, %d after reopen; want equal and > 0",
			afterSnapshot, e.snapshotSize)
	}
}

// Once the data outgrows the threshold, the wal must reach the snapshot's
// size before the next snapshot, so snapshot cost stays proportional to writes.
func TestTriggerFollowsSnapshotSize(t *testing.T) {
	const threshold = 64
	dir := t.TempDir()
	eng, err := NewWALEngine(dir, threshold)
	if err != nil {
		t.Fatal(err)
	}
	e := eng.(*walEngine)
	defer e.Close()

	// 20 distinct keys: far more data than the threshold.
	for i := range 20 {
		mustPut(t, e, fmt.Sprintf("key%02d", i), "value")
	}
	waitIdle(t, e)
	e.mu.Lock()
	size := e.snapshotSize
	e.mu.Unlock()
	if size <= threshold {
		t.Fatalf("snapshotSize = %d, want more than the threshold %d", size, threshold)
	}

	// Overwrites keep the data size fixed. Stay below the snapshot size.
	rec := int64(len(encodeRecord(operationPut, []byte("key00"), []byte("value"))))
	e.mu.Lock()
	start := e.walSize
	e.mu.Unlock()
	n := (size - start) / rec
	if (size-start)%rec == 0 {
		n--
	}
	for i := range n {
		mustPut(t, e, fmt.Sprintf("key%02d", i%20), "value")
	}
	waitIdle(t, e)
	e.mu.Lock()
	got := e.walSize
	e.mu.Unlock()
	if want := start + n*rec; got != want {
		t.Fatalf("walSize = %d, want %d: a snapshot ran before the wal reached the snapshot size %d", got, want, size)
	}

	// One more record crosses it.
	mustPut(t, e, "key00", "value")
	waitIdle(t, e)
	e.mu.Lock()
	got = e.walSize
	e.mu.Unlock()
	if got != 0 {
		t.Fatalf("walSize = %d after crossing the snapshot size %d, want 0", got, size)
	}
}

// waitIdle waits until no snapshot is pending or running.
func waitIdle(t *testing.T, e *walEngine) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		e.mu.Lock()
		busy := e.snapshotting
		e.mu.Unlock()
		if !busy {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("snapshot still running after 5s")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestThresholdMustBePositive(t *testing.T) {
	for _, threshold := range []int64{0, -1} {
		if _, err := NewWALEngine(t.TempDir(), threshold); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("threshold %d: err = %v, want ErrInvalidArgument", threshold, err)
		}
	}
}

func openEngine(t *testing.T, dir string) *walEngine {
	t.Helper()
	e, err := NewWALEngine(dir, noSnapshot)
	if err != nil {
		t.Fatalf("NewWALEngine: %v", err)
	}
	return e.(*walEngine)
}

func mustSnapshot(t *testing.T, e *walEngine) {
	t.Helper()
	if err := e.snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
}

func mustPut(t *testing.T, e *walEngine, k, v string) {
	t.Helper()
	if err := e.Put([]byte(k), []byte(v)); err != nil {
		t.Fatalf("Put(%q): %v", k, err)
	}
}

func wantStore(t *testing.T, e *walEngine, want map[string]string) {
	t.Helper()
	if len(e.storage) != len(want) {
		t.Fatalf("store has %d keys, want %d (%v)", len(e.storage), len(want), want)
	}
	for k, v := range want {
		if got := string(e.storage[k]); got != v {
			t.Fatalf("%s = %q, want %q", k, got, v)
		}
	}
}

func sizeOf(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

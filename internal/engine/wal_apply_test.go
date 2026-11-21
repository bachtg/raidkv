package engine

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

func putRec(seq int64, k, v string) Record {
	return Record{Seq: seq, Key: []byte(k), Value: []byte(v)}
}

func mustApply(t *testing.T, e *walEngine, recs ...Record) {
	t.Helper()
	for _, r := range recs {
		if err := e.Apply(r); err != nil {
			t.Fatalf("Apply(seq %d): %v", r.Seq, err)
		}
	}
}

// Apply keeps seqs, skips duplicates, rejects gaps, survives a reopen.
func TestApply(t *testing.T) {
	forEachMode(t, func(t *testing.T, mode SyncMode) {
		dir := t.TempDir()
		eng, err := NewWALEngine(dir, Options{SnapshotThreshold: noSnapshot, Sync: mode})
		if err != nil {
			t.Fatal(err)
		}
		e := eng.(*walEngine)

		mustApply(t, e, putRec(1, "a", "1"), putRec(2, "b", "2"), putRec(3, "a", "3"))
		// 2 and 3 again: skipped.
		mustApply(t, e, putRec(2, "b", "stale"), putRec(3, "a", "stale"),
			Record{Seq: 4, Delete: true, Key: []byte("b")}, putRec(5, "c", "5"))

		if err := e.Apply(putRec(7, "x", "x")); !errors.Is(err, ErrSeqGap) {
			t.Fatalf("Apply(seq 7) after 5 = %v, want ErrSeqGap", err)
		}
		if got := e.LastSeq(); got != 5 {
			t.Fatalf("LastSeq = %d, want 5", got)
		}

		check := func(e *walEngine) {
			t.Helper()
			wantStore(t, e, map[string]string{"a": "3", "c": "5"})
		}
		check(e)
		e.Close()

		wantConsecutive(t, fileSeqs(t, filepath.Join(dir, walFileName)), 1)
		if n := len(fileSeqs(t, filepath.Join(dir, walFileName))); n != 5 {
			t.Fatalf("wal has %d records, want 5 (no duplicates)", n)
		}

		eng, err = NewWALEngine(dir, Options{SnapshotThreshold: noSnapshot, Sync: mode})
		if err != nil {
			t.Fatal(err)
		}
		e = eng.(*walEngine)
		defer e.Close()
		check(e)
		if got := e.LastSeq(); got != 5 {
			t.Fatalf("LastSeq after reopen = %d, want 5", got)
		}
	})
}

// Seqs carry on across the backup's own snapshot and a reopen.
func TestApplyAcrossSnapshot(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir)
	for i := int64(1); i <= 10; i++ {
		mustApply(t, e, putRec(i, fmt.Sprintf("k%d", i%3), fmt.Sprint(i)))
	}
	mustSnapshot(t, e)
	mustApply(t, e, putRec(11, "n", "11"))
	e.Close()

	e = openEngine(t, dir)
	defer e.Close()
	if got := e.LastSeq(); got != 11 {
		t.Fatalf("LastSeq = %d, want 11", got)
	}
	mustApply(t, e, putRec(12, "n", "12"))
	if got := seqsOf(mustReadFrom(t, e, 11, 10)); fmt.Sprint(got) != "[11 12]" {
		t.Fatalf("ReadFrom(11) = %v, want [11 12]", got)
	}
	if _, err := e.ReadFrom(10, 10); !errors.Is(err, ErrTooOld) {
		t.Fatalf("ReadFrom(10) err = %v, want ErrTooOld", err)
	}
}

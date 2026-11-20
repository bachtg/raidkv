package engine

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// fileSeqs returns the seq of every whole record in path, in file order.
func fileSeqs(t *testing.T, path string) []int64 {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	var seqs []int64
	for len(data) >= 21 {
		seq := int64(binary.BigEndian.Uint64(data[4:12]))
		keyLen := int(binary.BigEndian.Uint32(data[13:17]))
		valueLen := int(binary.BigEndian.Uint32(data[17+keyLen : 21+keyLen]))
		seqs = append(seqs, seq)
		data = data[21+keyLen+valueLen:]
	}
	if len(data) != 0 {
		t.Fatalf("%s: %d trailing bytes", path, len(data))
	}
	return seqs
}

// snapshotSeq returns the seq in the snapshot end record, or 0 with no snapshot.
func snapshotSeq(t *testing.T, dir string) int64 {
	t.Helper()
	seqs := fileSeqs(t, filepath.Join(dir, snapshotFileName))
	if len(seqs) == 0 {
		return 0
	}
	return seqs[len(seqs)-1]
}

func lastSeq(e *walEngine) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.sequenceNumber
}

func wantConsecutive(t *testing.T, seqs []int64, from int64) {
	t.Helper()
	for i, s := range seqs {
		if s != from+int64(i) {
			t.Fatalf("seq #%d = %d, want %d (seqs %v)", i, s, from+int64(i), seqs)
		}
	}
}

// Seqs start at 1, go up by one, and carry on after a restart.
func TestSeqSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir)
	for i := range 5 {
		mustPut(t, e, fmt.Sprintf("k%d", i), "v")
	}
	if got := lastSeq(e); got != 5 {
		t.Fatalf("seq = %d, want 5", got)
	}
	e.Close()

	e = openEngine(t, dir)
	if got := lastSeq(e); got != 5 {
		t.Fatalf("seq after reopen = %d, want 5", got)
	}
	if err := e.Delete([]byte("k0")); err != nil {
		t.Fatal(err)
	}
	e.Close()

	wantConsecutive(t, fileSeqs(t, filepath.Join(dir, walFileName)), 1)
	if n := len(fileSeqs(t, filepath.Join(dir, walFileName))); n != 6 {
		t.Fatalf("wal has %d records, want 6", n)
	}
}

// Right after a snapshot the wal is empty; the seq must come from the
// snapshot, not restart at 1.
func TestSeqSurvivesSnapshotWithEmptyWAL(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir)
	for i := range 10 {
		mustPut(t, e, fmt.Sprintf("k%d", i%3), fmt.Sprint(i))
	}
	mustSnapshot(t, e)
	e.Close()

	seqs := fileSeqs(t, filepath.Join(dir, snapshotFileName))
	if len(seqs) != 4 || seqs[3] != 10 {
		t.Fatalf("snapshot seqs = %v, want 3 data records then end seq 10", seqs)
	}
	for _, s := range seqs[:3] {
		if s != 0 {
			t.Fatalf("snapshot seqs = %v, want 0 on data records", seqs)
		}
	}
	if n := sizeOf(t, filepath.Join(dir, walFileName)); n != 0 {
		t.Fatalf("wal size = %d after snapshot, want 0", n)
	}

	e = openEngine(t, dir)
	if got := lastSeq(e); got != 10 {
		t.Fatalf("seq after reopen = %d, want 10", got)
	}
	mustPut(t, e, "k", "v")
	e.Close()
	wantConsecutive(t, fileSeqs(t, filepath.Join(dir, walFileName)), 11)
}

// A torn last record is dropped, and so is its seq: the next write reuses it,
// leaving no gap.
func TestSeqTornTailLeavesNoGap(t *testing.T) {
	rec := len(encodeRecord(0, operationPut, []byte("k0"), []byte("v")))
	for _, cut := range []int{2, 4 + 3, rec - 1} { // in the crc, in the seq, in the value
		t.Run(fmt.Sprintf("keep%d", cut), func(t *testing.T) {
			dir := t.TempDir()
			e := openEngine(t, dir)
			for i := range 3 {
				mustPut(t, e, fmt.Sprintf("k%d", i), "v")
			}
			e.Close()

			wal := filepath.Join(dir, walFileName)
			if err := os.Truncate(wal, int64(2*rec+cut)); err != nil {
				t.Fatal(err)
			}

			e = openEngine(t, dir)
			if got := lastSeq(e); got != 2 {
				t.Fatalf("seq after torn tail = %d, want 2", got)
			}
			mustPut(t, e, "k9", "v")
			e.Close()
			wantConsecutive(t, fileSeqs(t, wal), 1)
		})
	}
}

// Concurrent writers with frequent snapshots, in every sync mode: the
// snapshot covers exactly 1..S, the wal holds S+1.. in file order with no
// gap or repeat, and the count matches the acked writes.
func TestSeqOrderUnderConcurrentWritesAndSnapshots(t *testing.T) {
	forEachMode(t, func(t *testing.T, mode SyncMode) {
		for _, threshold := range []int64{noSnapshot, 512} {
			dir := t.TempDir()
			eng, err := NewWALEngine(dir, Options{SnapshotThreshold: threshold, Sync: mode})
			if err != nil {
				t.Fatal(err)
			}
			e := eng.(*walEngine)

			const writers, ops = 8, 200
			var wg sync.WaitGroup
			for w := range writers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range ops {
						if err := e.Put(fmt.Appendf(nil, "w%d-%d", w, i%7), []byte("v")); err != nil {
							t.Error(err)
							return
						}
					}
				}()
			}
			wg.Wait()
			waitIdle(t, e)
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}

			const total = writers * ops
			snap := snapshotSeq(t, dir)
			walSeqs := fileSeqs(t, filepath.Join(dir, walFileName))
			wantConsecutive(t, walSeqs, snap+1)
			if got := snap + int64(len(walSeqs)); got != total {
				t.Fatalf("snapshot %d + wal %d records = %d, want %d", snap, len(walSeqs), got, total)
			}
			if threshold != noSnapshot && snap == 0 {
				t.Fatal("no snapshot ran")
			}

			e = openEngine(t, dir)
			if got := lastSeq(e); got != total {
				t.Fatalf("seq after reopen = %d, want %d", got, total)
			}
			e.Close()
		}
	})
}

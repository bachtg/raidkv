package engine

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func seqsOf(recs []Record) []int64 {
	out := make([]int64, len(recs))
	for i, r := range recs {
		out[i] = r.Seq
	}
	return out
}

func mustReadFrom(t *testing.T, e *walEngine, from int64, limit int) []Record {
	t.Helper()
	recs, err := e.ReadFrom(from, limit)
	if err != nil {
		t.Fatalf("ReadFrom(%d, %d): %v", from, limit, err)
	}
	return recs
}

func TestReadFromRange(t *testing.T) {
	e := openEngine(t, t.TempDir())
	defer e.Close()
	for i := 1; i <= 9; i++ {
		mustPut(t, e, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	if err := e.Delete([]byte("k3")); err != nil { // seq 10
		t.Fatal(err)
	}

	tests := []struct {
		from  int64
		limit int
		want  []int64
	}{
		{1, 3, []int64{1, 2, 3}},
		{5, 2, []int64{5, 6}},
		{5, 100, []int64{5, 6, 7, 8, 9, 10}},
		{10, 1, []int64{10}},
		{11, 5, nil}, // nothing new yet
	}
	for _, tt := range tests {
		got := seqsOf(mustReadFrom(t, e, tt.from, tt.limit))
		if fmt.Sprint(got) != fmt.Sprint(tt.want) {
			t.Errorf("ReadFrom(%d, %d) = %v, want %v", tt.from, tt.limit, got, tt.want)
		}
	}

	for _, limit := range []int{0, -1} {
		if _, err := e.ReadFrom(1, limit); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("ReadFrom(1, %d) err = %v, want ErrInvalidArgument", limit, err)
		}
	}

	recs := mustReadFrom(t, e, 9, 2)
	if r := recs[0]; r.Delete || string(r.Key) != "k9" || string(r.Value) != "v9" {
		t.Errorf("record 9 = %+v, want Put k9=v9", r)
	}
	if r := recs[1]; !r.Delete || string(r.Key) != "k3" || len(r.Value) != 0 {
		t.Errorf("record 10 = %+v, want Delete k3", r)
	}
}

// Records cut by a snapshot are gone: ErrTooOld up to the snapshot seq, the
// wal after it, before and after a reopen.
func TestReadFromTooOldAfterSnapshot(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir)
	for i := range 10 {
		mustPut(t, e, fmt.Sprintf("k%d", i), "v")
	}
	mustSnapshot(t, e)
	for i := range 3 {
		mustPut(t, e, fmt.Sprintf("n%d", i), "v")
	}

	check := func(e *walEngine) {
		t.Helper()
		for _, from := range []int64{1, 5, 10} {
			if _, err := e.ReadFrom(from, 10); !errors.Is(err, ErrTooOld) {
				t.Fatalf("ReadFrom(%d) err = %v, want ErrTooOld", from, err)
			}
		}
		if got := seqsOf(mustReadFrom(t, e, 11, 10)); fmt.Sprint(got) != "[11 12 13]" {
			t.Fatalf("ReadFrom(11) = %v, want [11 12 13]", got)
		}
	}
	check(e)
	e.Close()

	e = openEngine(t, dir)
	defer e.Close()
	check(e)
}

// Periodic acks before the fsync, so its writes are not readable until the
// sync. Group and always ack after the fsync, so every acked write is.
func TestReadFromOnlySynced(t *testing.T) {
	forEachMode(t, func(t *testing.T, mode SyncMode) {
		eng, err := NewWALEngine(t.TempDir(), Options{SnapshotThreshold: noSnapshot, Sync: mode, SyncInterval: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		e := eng.(*walEngine)
		defer e.Close()

		for i := range 5 {
			mustPut(t, e, fmt.Sprintf("k%d", i), "v")
		}

		got := mustReadFrom(t, e, 1, 10)
		if mode == SyncPeriodic {
			if len(got) != 0 {
				t.Fatalf("read %v before any fsync, want nothing", seqsOf(got))
			}
			e.mu.Lock()
			e.waitDurable(e.written)
			e.mu.Unlock()
			got = mustReadFrom(t, e, 1, 10)
		}
		if fmt.Sprint(seqsOf(got)) != "[1 2 3 4 5]" {
			t.Fatalf("ReadFrom(1) = %v, want [1 2 3 4 5]", seqsOf(got))
		}
	})
}

// Readers follow the wal while writers write and snapshots keep cutting it.
// Each read must start at the seq asked for and have no gap; when the records
// are cut, the reader jumps past the snapshot like a backup would.
func TestReadFromDuringWritesAndSnapshots(t *testing.T) {
	forEachMode(t, func(t *testing.T, mode SyncMode) {
		eng, err := NewWALEngine(t.TempDir(), Options{SnapshotThreshold: 2048, Sync: mode, SyncInterval: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		e := eng.(*walEngine)
		defer e.Close()

		var stop atomic.Bool
		var writers, readers sync.WaitGroup
		for w := range 4 {
			writers.Add(1)
			go func() {
				defer writers.Done()
				for i := 0; i < 2000; i++ {
					k := fmt.Sprintf("w%d-%d", w, i%50)
					if err := e.Put([]byte(k), []byte(k+"!")); err != nil {
						t.Error(err)
						return
					}
				}
			}()
		}

		var reads, tooOld atomic.Int64
		for range 4 {
			readers.Add(1)
			go func() {
				defer readers.Done()
				next := int64(1)
				for !stop.Load() {
					recs, err := e.ReadFrom(next, 64)
					if errors.Is(err, ErrTooOld) {
						tooOld.Add(1)
						e.mu.RLock()
						next = e.snapshotSeq + 1 // a backup would load the snapshot here
						e.mu.RUnlock()
						continue
					}
					if err != nil {
						t.Errorf("ReadFrom(%d): %v", next, err)
						return
					}
					for i, r := range recs {
						if r.Seq != next+int64(i) {
							t.Errorf("ReadFrom(%d) = %v, want consecutive from %d", next, seqsOf(recs), next)
							return
						}
						if string(r.Value) != string(r.Key)+"!" {
							t.Errorf("record %d: key %q value %q", r.Seq, r.Key, r.Value)
							return
						}
					}
					reads.Add(int64(len(recs)))
					next += int64(len(recs))
				}
			}()
		}

		writers.Wait()
		stop.Store(true)
		readers.Wait()

		e.mu.RLock()
		snaps := e.walGeneration
		e.mu.RUnlock()
		if snaps == 0 {
			t.Fatal("no snapshot ran")
		}
		t.Logf("%d snapshots, %d records read, %d ErrTooOld", snaps, reads.Load(), tooOld.Load())
	})
}

// Each call opens the wal; none may leak the file.
func TestReadFromClosesFile(t *testing.T) {
	if _, err := os.ReadDir("/proc/self/fd"); err != nil {
		t.Skip("no /proc/self/fd")
	}
	e := openEngine(t, t.TempDir())
	defer e.Close()
	mustPut(t, e, "k", "v")

	fds := func() int {
		ents, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		return len(ents)
	}
	before := fds()
	for range 1000 {
		mustReadFrom(t, e, 1, 10)
		mustReadFrom(t, e, 2, 10) // nothing new
	}
	if after := fds(); after > before+5 {
		t.Fatalf("open fds %d -> %d after 2000 reads", before, after)
	}
}

package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func openPeriodic(t *testing.T, dir string, interval time.Duration) *walEngine {
	t.Helper()
	e, err := NewWALEngine(dir, Options{SnapshotThreshold: noSnapshot, Sync: SyncPeriodic, SyncInterval: interval})
	if err != nil {
		t.Fatal(err)
	}
	return e.(*walEngine)
}

// Each record is written once and is readable before any fsync.
func TestPeriodicWritesOnceAndAppliesAtOnce(t *testing.T) {
	dir := t.TempDir()
	e := openPeriodic(t, dir, time.Hour)
	defer e.Close()

	var size int64
	for i := range 50 {
		k, v := fmt.Sprintf("k%d", i), "v"
		size += int64(len(encodeRecord(operationPut, []byte(k), []byte(v))))
		mustPut(t, e, k, v)
		if got, err := e.Get([]byte(k)); err != nil || string(got) != v {
			t.Fatalf("Get(%s) right after Put = %q, %v; want %q", k, got, err, v)
		}
	}

	e.mu.Lock()
	written, synced := e.written, e.synced
	e.mu.Unlock()
	if written != size {
		t.Fatalf("written = %d, want %d", written, size)
	}
	if synced != 0 {
		t.Fatalf("synced = %d before the first tick, want 0", synced)
	}
	if fi, err := os.Stat(filepath.Join(dir, walFileName)); err != nil || fi.Size() != size {
		t.Fatalf("wal.log size = %v, %v; want %d", fi.Size(), err, size)
	}
}

// With no Close, the background loop catches synced up to written.
func TestPeriodicLoopSyncs(t *testing.T) {
	e := openPeriodic(t, t.TempDir(), time.Millisecond)
	defer e.Close()

	for i := range 20 {
		mustPut(t, e, fmt.Sprintf("k%d", i), "v")
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		e.mu.Lock()
		done := e.synced == e.written
		e.mu.Unlock()
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("synced never caught up with written")
		}
		time.Sleep(time.Millisecond)
	}
}

// Close races the ticker and the writers: no deadlock, the loop exits, and
// everything acked is in the wal.
func TestPeriodicCloseRacesTicker(t *testing.T) {
	for range 50 {
		dir := t.TempDir()
		e := openPeriodic(t, dir, 50*time.Microsecond)

		var wg sync.WaitGroup
		for w := range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; ; i++ {
					if e.Put(fmt.Appendf(nil, "w%d", w), fmt.Appendf(nil, "%d", i)) != nil {
						return
					}
				}
			}()
		}
		time.Sleep(time.Millisecond)

		closed := make(chan error)
		go func() { closed <- e.Close() }()
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Close deadlocked")
		}
		wg.Wait()

		select {
		case <-e.periodicDone:
		default:
			t.Fatal("sync loop still running after Close")
		}
		if e.synced != e.written {
			t.Fatalf("after Close synced = %d, written = %d", e.synced, e.written)
		}

		want := map[string]string{}
		for k, v := range e.storage {
			want[k] = string(v)
		}
		e2 := openEngine(t, dir)
		wantStore(t, e2, want)
		e2.Close()
	}
}

func TestPeriodicCloseTwice(t *testing.T) {
	e := openPeriodic(t, t.TempDir(), time.Millisecond)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Get([]byte("k")); err != ErrNotFound {
		t.Fatalf("Get after double Close = %v, want ErrNotFound (lock not left held)", err)
	}
}

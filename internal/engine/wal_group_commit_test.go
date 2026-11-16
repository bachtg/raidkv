package engine

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Writers race with snapshots that fire every few records. Every acked
// write must survive a reopen.
func TestConcurrentWritesWithSnapshots(t *testing.T) {
	forEachMode(t, func(t *testing.T, mode SyncMode) {
		dir := t.TempDir()
		e, err := NewWALEngine(dir, Options{SnapshotThreshold: 256, Sync: mode})
		if err != nil {
			t.Fatal(err)
		}

		const writers, ops = 8, 300
		want := make([]map[string]string, writers)
		var wg sync.WaitGroup
		for w := range writers {
			want[w] = map[string]string{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range ops {
					k := fmt.Sprintf("w%d-k%d", w, i%10)
					if i%3 == 2 {
						if err := e.Delete([]byte(k)); err != nil {
							t.Errorf("Delete(%s): %v", k, err)
							return
						}
						delete(want[w], k)
						continue
					}
					v := fmt.Sprintf("v%d", i)
					if err := e.Put([]byte(k), []byte(v)); err != nil {
						t.Errorf("Put(%s): %v", k, err)
						return
					}
					want[w][k] = v
				}
			}()
		}
		wg.Wait()

		all := map[string]string{}
		for _, m := range want {
			for k, v := range m {
				all[k] = v
			}
		}
		wantStore(t, e.(*walEngine), all)
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}

		e2 := openEngine(t, dir)
		defer e2.Close()
		wantStore(t, e2, all)
	})
}

func forEachMode(t *testing.T, test func(*testing.T, SyncMode)) {
	for _, m := range []SyncMode{SyncGroup, SyncAlways, SyncPeriodic} {
		t.Run(m.String(), func(t *testing.T) { test(t, m) })
	}
}

// Close while writers are still waiting for fsync: no panic, and every
// write that returned nil is on disk.
func TestCloseDuringWrites(t *testing.T) {
	forEachMode(t, func(t *testing.T, mode SyncMode) {
		for round := range 20 {
			dir := t.TempDir()
			e, err := NewWALEngine(dir, Options{SnapshotThreshold: 512, Sync: mode, SyncInterval: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}

			const writers = 8
			acked := make([]map[string]string, writers)
			var wg sync.WaitGroup
			for w := range writers {
				acked[w] = map[string]string{}
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; ; i++ {
						k, v := fmt.Sprintf("w%d-k%d", w, i%5), fmt.Sprintf("v%d", i)
						err := e.Put([]byte(k), []byte(v))
						if errors.Is(err, ErrClosed) {
							return
						}
						if err != nil {
							t.Errorf("Put(%s): %v", k, err)
							return
						}
						acked[w][k] = v
					}
				}()
			}

			time.Sleep(time.Duration(5+round) * time.Millisecond)
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			wg.Wait()

			all := map[string]string{}
			for _, m := range acked {
				for k, v := range m {
					all[k] = v
				}
			}
			e2 := openEngine(t, dir)
			wantStore(t, e2, all)
			e2.Close()
		}
	})
}

// After the writers are done nothing is left half way: all written bytes are
// synced and applied.
func TestNothingPendingAfterWrites(t *testing.T) {
	e := openEngine(t, t.TempDir())
	defer e.Close()

	var wg sync.WaitGroup
	for w := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 50 {
				mustPut(t, e, fmt.Sprintf("w%d-%d", w, i), "v")
			}
		}()
	}
	wg.Wait()

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.synced != e.written || len(e.pendingRecords) != 0 || e.isSyncing {
		t.Fatalf("synced %d, written %d, pending %d, syncing %v; want all written synced and applied",
			e.synced, e.written, len(e.pendingRecords), e.isSyncing)
	}
	if len(e.storage) != 16*50 {
		t.Fatalf("store has %d keys, want %d", len(e.storage), 16*50)
	}
}

// A snapshot waiting for a leader's fsync must hold back new writes: a Delete
// that slips in is cut from the wal while the snapshot still has the key.
func TestSnapshotHoldsBackWritesWhileDraining(t *testing.T) {
	for range 30 {
		dir := t.TempDir()
		e := openEngine(t, dir)
		mustPut(t, e, "k", "v")

		// A leader is mid fsync of one more byte, and a snapshot is due.
		e.mu.Lock()
		e.written++
		leaderEnd := e.written
		e.isSyncing = true
		e.snapshotting = true
		e.mu.Unlock()

		snapDone := make(chan error)
		go func() { snapDone <- e.snapshot() }()
		time.Sleep(5 * time.Millisecond) // snapshot is now waiting on the leader

		delDone := make(chan error)
		go func() { delDone <- e.Delete([]byte("k")) }()
		time.Sleep(5 * time.Millisecond)

		// The leader finishes.
		e.mu.Lock()
		e.synced = leaderEnd
		e.isSyncing = false
		e.syncCond.Broadcast()
		e.mu.Unlock()

		if err := <-snapDone; err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		if err := <-delDone; err != nil {
			t.Fatalf("Delete: %v", err)
		}
		e.Close()

		e = openEngine(t, dir)
		wantStore(t, e, map[string]string{})
		e.Close()
	}
}

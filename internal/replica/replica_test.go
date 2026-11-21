package replica_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	api "github.com/bachtg/raidkv/api/raidkv/v1"
	"github.com/bachtg/raidkv/internal/engine"
	"github.com/bachtg/raidkv/internal/replica"
	"github.com/bachtg/raidkv/internal/server"
)

const noSnapshot = 1 << 40

var modes = []engine.SyncMode{engine.SyncGroup, engine.SyncAlways, engine.SyncPeriodic}

type walEngine interface {
	engine.Replica
	engine.WALReader
}

func openWAL(t *testing.T, dir string, opts engine.Options) walEngine {
	t.Helper()
	if opts.SyncInterval == 0 {
		opts.SyncInterval = 5 * time.Millisecond
	}
	e, err := engine.NewWALEngine(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	return e.(walEngine)
}

// serve returns the address it listens on and a func that stops it.
func serve(t *testing.T, e engine.Engine, addr string) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	g, err := server.NewGRPCServer(e)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	return lis.Addr().String(), g.Stop
}

func dial(t *testing.T, addr string) api.ReplicationServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 10 * time.Millisecond, Multiplier: 1.6, MaxDelay: 100 * time.Millisecond},
			MinConnectTimeout: time.Second,
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return api.NewReplicationServiceClient(conn)
}

// puller runs replica.Run in the background.
type puller struct {
	cancel context.CancelFunc
	done   chan struct{} // closed when Run returns
	err    error
}

func pull(primary api.ReplicationServiceClient, backup engine.Replica, limit int) *puller {
	ctx, cancel := context.WithCancel(context.Background())
	p := &puller{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(p.done)
		p.err = replica.Run(ctx, primary, backup, replica.Config{Limit: limit, PollInterval: time.Millisecond})
	}()
	return p
}

func (p *puller) stop() error {
	p.cancel()
	<-p.done
	return p.err
}

func waitCaughtUp(t *testing.T, primary, backup engine.Replica, run *puller) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for backup.LastSeq() != primary.LastSeq() {
		select {
		case <-run.done:
			t.Fatalf("Run stopped at seq %d of %d: %v", backup.LastSeq(), primary.LastSeq(), run.err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("backup at seq %d, primary at %d after 10s", backup.LastSeq(), primary.LastSeq())
		}
		time.Sleep(time.Millisecond)
	}
}

func sameWAL(t *testing.T, primary, backup engine.WALReader) {
	t.Helper()
	p, err := primary.ReadFrom(1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	b, err := backup.ReadFrom(1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(p) == 0 || len(p) != len(b) {
		t.Fatalf("primary has %d records, backup %d", len(p), len(b))
	}
	for i := range p {
		if fmt.Sprint(p[i]) != fmt.Sprint(b[i]) {
			t.Fatalf("record %d: primary %+v, backup %+v", i, p[i], b[i])
		}
	}
}

func sameData(t *testing.T, primary, backup engine.Engine, keys []string) {
	t.Helper()
	for _, k := range keys {
		pv, perr := primary.Get([]byte(k))
		bv, berr := backup.Get([]byte(k))
		if string(pv) != string(bv) || !errors.Is(berr, perr) {
			t.Fatalf("key %s: primary %q %v, backup %q %v", k, pv, perr, bv, berr)
		}
	}
}

// write returns every key touched.
func write(t *testing.T, e engine.Engine, writers, ops int, prefix string) []string {
	t.Helper()
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range ops {
				k := []byte(fmt.Sprintf("%s%d-%d", prefix, w, i%20))
				var err error
				if i%7 == 6 {
					err = e.Delete(k)
				} else {
					err = e.Put(k, []byte(fmt.Sprint(i)))
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	var keys []string
	for w := range writers {
		for i := range 20 {
			keys = append(keys, fmt.Sprintf("%s%d-%d", prefix, w, i))
		}
	}
	return keys
}

// Concurrent writes reach the backup with the same seqs, in every mode.
func TestReplicate(t *testing.T) {
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			p := openWAL(t, t.TempDir(), engine.Options{SnapshotThreshold: noSnapshot, Sync: mode})
			defer p.Close()
			b := openWAL(t, t.TempDir(), engine.Options{SnapshotThreshold: noSnapshot, Sync: mode})
			defer b.Close()
			addr, _ := serve(t, p, "127.0.0.1:0")

			run := pull(dial(t, addr), b, 7)
			keys := write(t, p, 4, 300, "k")
			waitCaughtUp(t, p, b, run)
			if err := run.stop(); !errors.Is(err, context.Canceled) {
				t.Fatalf("Run after cancel = %v, want context.Canceled", err)
			}

			sameWAL(t, p, b)
			sameData(t, p, b, keys)
		})
	}
}

func TestReplicateWhileBackupSnapshots(t *testing.T) {
	p := openWAL(t, t.TempDir(), engine.Options{SnapshotThreshold: noSnapshot})
	defer p.Close()
	bdir := t.TempDir()
	b := openWAL(t, bdir, engine.Options{SnapshotThreshold: 2048})
	addr, _ := serve(t, p, "127.0.0.1:0")

	run := pull(dial(t, addr), b, 50)
	keys := write(t, p, 4, 500, "k")
	waitCaughtUp(t, p, b, run)
	_ = run.stop()
	sameData(t, p, b, keys)

	if _, err := b.ReadFrom(1, 10); !errors.Is(err, engine.ErrTooOld) {
		t.Fatalf("backup ReadFrom(1) = %v, want ErrTooOld: the backup never snapshotted", err)
	}
	last := b.LastSeq()
	b.Close()

	b = openWAL(t, bdir, engine.Options{SnapshotThreshold: 2048})
	defer b.Close()
	if got := b.LastSeq(); got != last {
		t.Fatalf("backup LastSeq after reopen = %d, want %d", got, last)
	}
	sameData(t, p, b, keys)
}

func TestBackupRestartResumes(t *testing.T) {
	p := openWAL(t, t.TempDir(), engine.Options{SnapshotThreshold: noSnapshot})
	defer p.Close()
	bdir := t.TempDir()
	b := openWAL(t, bdir, engine.Options{SnapshotThreshold: noSnapshot})
	addr, _ := serve(t, p, "127.0.0.1:0")
	client := dial(t, addr)

	run := pull(client, b, 100)
	keys := write(t, p, 2, 100, "a")
	waitCaughtUp(t, p, b, run)
	_ = run.stop()
	b.Close()

	keys = append(keys, write(t, p, 2, 100, "b")...) // while the backup is down

	b = openWAL(t, bdir, engine.Options{SnapshotThreshold: noSnapshot})
	defer b.Close()
	run = pull(client, b, 100)
	defer run.stop()
	waitCaughtUp(t, p, b, run)
	sameWAL(t, p, b)
	sameData(t, p, b, keys)
}

// Run retries while the primary is down.
func TestRunRetriesWhilePrimaryDown(t *testing.T) {
	p := openWAL(t, t.TempDir(), engine.Options{SnapshotThreshold: noSnapshot})
	defer p.Close()
	b := openWAL(t, t.TempDir(), engine.Options{SnapshotThreshold: noSnapshot})
	defer b.Close()
	addr, stopServer := serve(t, p, "127.0.0.1:0")

	run := pull(dial(t, addr), b, 100)
	defer run.stop()
	keys := write(t, p, 1, 50, "a")
	waitCaughtUp(t, p, b, run)

	stopServer()
	keys = append(keys, write(t, p, 1, 50, "b")...)
	time.Sleep(300 * time.Millisecond)
	select {
	case <-run.done:
		t.Fatalf("Run returned while the primary was down: %v", run.err)
	default:
	}

	serve(t, p, addr)
	waitCaughtUp(t, p, b, run)
	sameData(t, p, b, keys)
}

// A new backup cannot catch up once the primary has snapshotted.
func TestRunStopsOnOutOfRange(t *testing.T) {
	p := openWAL(t, t.TempDir(), engine.Options{SnapshotThreshold: 1})
	defer p.Close()
	b := openWAL(t, t.TempDir(), engine.Options{SnapshotThreshold: noSnapshot})
	defer b.Close()

	for i := 0; ; i++ {
		if err := p.Put([]byte("k"), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if _, err := p.ReadFrom(1, 1); errors.Is(err, engine.ErrTooOld) {
			break
		}
		if i == 1000 {
			t.Fatal("primary never snapshotted")
		}
	}
	addr, _ := serve(t, p, "127.0.0.1:0")

	run := pull(dial(t, addr), b, 100)
	select {
	case <-run.done:
		if status.Code(run.err) != codes.OutOfRange {
			t.Fatalf("Run = %v, want OutOfRange", run.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run still running after 5s, want it to stop on OutOfRange")
	}
	if got := b.LastSeq(); got != 0 {
		t.Fatalf("backup LastSeq = %d, want 0 (nothing applied)", got)
	}
}

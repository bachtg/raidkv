//go:build crash

package crash_test

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/bachtg/raidkv/internal/client"
)

var (
	rounds  = flag.Int("rounds", 20, "number of kill -9 rounds")
	clients = flag.Int("clients", 8, "concurrent clients")
	keys    = flag.Int("keys", 64, "number of keys, split between clients")
	baseDir = flag.String("dir", "", "parent of the data dir; use a real disk, not tmpfs (default: a temp dir)")
	seed    = flag.Int64("seed", 0, "random seed (default: time based)")
)

var serverBin string

func TestMain(m *testing.M) {
	flag.Parse()
	binDir, err := os.MkdirTemp("", "raidkv-crash-bin")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	serverBin = filepath.Join(binDir, "raidkv-server")
	build := exec.Command("go", "build", "-o", serverBin, "github.com/bachtg/raidkv/cmd/raidkv-server")
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build raidkv-server:", err)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(binDir)
	os.Exit(code)
}

// value is what a key holds: a string, or absent after a Delete.
type value struct {
	s       string
	present bool
}

func (v value) String() string {
	if !v.present {
		return "<absent>"
	}
	return v.s
}

// keyState tracks what a key may hold after a crash. sure is the result of the
// last acknowledged write (or of the last read after a restart); maybe holds
// writes sent after it whose outcome is unknown.
type keyState struct {
	sure  value
	maybe []value
}

func (k *keyState) allowed(v value) bool {
	if v == k.sure {
		return true
	}
	for _, m := range k.maybe {
		if v == m {
			return true
		}
	}
	return false
}

type stats struct {
	ok, unknown, rejected int
}

func TestCrash(t *testing.T) {
	s := *seed
	if s == 0 {
		s = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(s))
	t.Logf("seed %d (rerun with -seed=%d)", s, s)

	parent := *baseDir
	if parent != "" {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	dir, err := os.MkdirTemp(parent, "raidkv-crash-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("data dir %s", dir)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("data dir kept for inspection: %s", dir)
			return
		}
		os.RemoveAll(dir)
	})

	state := make([]keyState, *keys)
	var total stats

	for round := 1; round <= *rounds; round++ {
		srv := startServer(t, dir)
		c, err := client.Dial(srv.addr)
		if err != nil {
			t.Fatal(err)
		}

		verify(t, c, state, round, srv)

		killAfter := time.Duration(20+rng.Intn(480)) * time.Millisecond
		roundSeed := rng.Int63()
		var (
			wg   sync.WaitGroup
			mu   sync.Mutex
			stop = make(chan struct{})
			rs   stats
		)
		for w := range *clients {
			wg.Add(1)
			go func() {
				defer wg.Done()
				st := runClient(c, state, w, roundSeed+int64(w), roundLabel(round), stop)
				mu.Lock()
				rs.ok += st.ok
				rs.unknown += st.unknown
				rs.rejected += st.rejected
				mu.Unlock()
			}()
		}

		time.Sleep(killAfter)
		srv.kill(t)
		close(stop)
		wg.Wait()
		c.Close()

		total.ok += rs.ok
		total.unknown += rs.unknown
		t.Logf("round %d: killed after %v, %d ok, %d unknown", round, killAfter, rs.ok, rs.unknown)
	}

	srv := startServer(t, dir)
	c, err := client.Dial(srv.addr)
	if err != nil {
		t.Fatal(err)
	}
	verify(t, c, state, *rounds+1, srv)
	c.Close()
	srv.kill(t)

	t.Logf("%d rounds, %d acknowledged writes, %d with unknown outcome, all keys consistent",
		*rounds, total.ok, total.unknown)
}

func roundLabel(round int) string { return fmt.Sprintf("r%d", round) }

// runClient sends Put and Delete, one at a time, to the keys owned by client w
// (key index i with i % clients == w) until the server dies or stop closes.
func runClient(c *client.Client, state []keyState, w int, seed int64, label string, stop <-chan struct{}) stats {
	var (
		rng = rand.New(rand.NewSource(seed))
		st  stats
		own []int
	)
	for i := w; i < len(state); i += *clients {
		own = append(own, i)
	}
	if len(own) == 0 {
		return st
	}

	for seq := 0; ; seq++ {
		select {
		case <-stop:
			return st
		default:
		}

		i := own[rng.Intn(len(own))]
		key := []byte(fmt.Sprintf("k%d", i))
		var (
			next value
			err  error
		)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if rng.Intn(10) < 7 {
			next = value{s: fmt.Sprintf("%s-c%d-%d", label, w, seq), present: true}
			err = c.Put(ctx, key, []byte(next.s))
		} else {
			err = c.Delete(ctx, key)
		}
		cancel()

		ks := &state[i]
		switch {
		case err == nil:
			ks.sure, ks.maybe = next, nil
			st.ok++
		case status.Code(err) == codes.InvalidArgument:
			st.rejected++ // the server refused it: it did not happen
		default:
			// Connection lost or timed out: the write may or may not have
			// been applied. The server is most likely gone, so stop.
			ks.maybe = append(ks.maybe, next)
			st.unknown++
			return st
		}
	}
}

// verify reads every key and checks it holds a value it is allowed to hold.
// What it reads becomes the new known state.
func verify(t *testing.T, c *client.Client, state []keyState, round int, srv *server) {
	t.Helper()
	for i := range state {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		raw, err := c.Get(ctx, []byte(fmt.Sprintf("k%d", i)))
		cancel()

		var got value
		switch {
		case err == nil:
			got = value{s: string(raw), present: true}
		case errors.Is(err, client.ErrNotFound):
		default:
			t.Fatalf("before round %d: Get k%d: %v\nserver log:\n%s", round, i, err, srv.log())
		}

		ks := &state[i]
		if !ks.allowed(got) {
			t.Fatalf("before round %d: k%d = %v, want %v or one of %v (acknowledged write lost or wrong value)\nserver log:\n%s",
				round, i, got, ks.sure, ks.maybe, srv.log())
		}
		ks.sure, ks.maybe = got, nil
	}
}

type server struct {
	cmd    *exec.Cmd
	addr   string
	mu     sync.Mutex
	stderr bytes.Buffer
	exited chan struct{}
}

func (s *server) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stderr.Write(p)
}

func (s *server) log() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stderr.String()
}

func startServer(t *testing.T, dir string) *server {
	t.Helper()
	s := &server{addr: freeAddr(t), exited: make(chan struct{})}
	s.cmd = exec.Command(serverBin, "-addr", s.addr, "-engine", "wal", "-data-dir", dir)
	s.cmd.Stdout, s.cmd.Stderr = s, s
	if err := s.cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	go func() {
		_ = s.cmd.Wait()
		close(s.exited)
	}()

	c, err := client.Dial(s.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case <-s.exited:
			t.Fatalf("server exited during startup:\n%s", s.log())
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, err := c.Get(ctx, []byte("ready-probe"))
		cancel()
		if err == nil || errors.Is(err, client.ErrNotFound) {
			return s
		}
		if time.Now().After(deadline) {
			s.cmd.Process.Kill()
			t.Fatalf("server not ready after 10s: %v\n%s", err, s.log())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// kill sends SIGKILL. It fails the test if the server had already exited on
// its own, which means it crashed or panicked.
func (s *server) kill(t *testing.T) {
	t.Helper()
	select {
	case <-s.exited:
		t.Fatalf("server exited before being killed:\n%s", s.log())
	default:
	}
	if err := s.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill server: %v", err)
	}
	<-s.exited
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

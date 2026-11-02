package server_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/bachtg/raidkv/internal/client"
	"github.com/bachtg/raidkv/internal/engine"
	"github.com/bachtg/raidkv/internal/server"
)

func newClient(t *testing.T) *client.Client {
	t.Helper()
	return newClientWith(t, engine.NewMemEngine())
}

// newClientWith runs a server backed by e over an in-memory connection.
func newClientWith(t *testing.T, e engine.Engine) *client.Client {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	g, err := server.NewGRPCServer(e)
	if err != nil {
		t.Fatalf("NewGRPCServer: %v", err)
	}
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c := client.New(conn)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

func TestPutThenGet(t *testing.T) {
	c := newClient(t)
	if err := c.Put(ctx(t), []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := c.Get(ctx(t), []byte("k"))
	if err != nil || string(got) != "v" {
		t.Fatalf("Get = %q, %v; want %q", got, err, "v")
	}
}

func TestGetMissingIsNotFound(t *testing.T) {
	c := newClient(t)
	_, err := c.Get(ctx(t), []byte("nope"))
	if !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("Get = %v; want client.ErrNotFound (server must return codes.NotFound)", err)
	}
}

func TestDelete(t *testing.T) {
	c := newClient(t)
	if err := c.Put(ctx(t), []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := c.Delete(ctx(t), []byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := c.Get(ctx(t), []byte("k")); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("Get after Delete = %v; want ErrNotFound", err)
	}
	if err := c.Delete(ctx(t), []byte("k")); err != nil {
		t.Fatalf("Delete of missing key = %v; want nil", err)
	}
}

func TestEmptyValueRoundTrips(t *testing.T) {
	c := newClient(t)
	if err := c.Put(ctx(t), []byte("k"), nil); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := c.Get(ctx(t), []byte("k"))
	if err != nil || len(got) != 0 {
		t.Fatalf("Get = %q, %v; want empty value, nil error", got, err)
	}
}

// maxKeyLen must match max_len on the key fields in raidkv.proto.
const maxKeyLen = 10000

func TestInvalidKeyIsInvalidArgument(t *testing.T) {
	c := newClient(t)
	keys := map[string][]byte{
		"empty":    nil,
		"too long": bytes.Repeat([]byte("k"), maxKeyLen+1),
	}
	for keyName, key := range keys {
		calls := map[string]func() error{
			"Get":    func() error { _, err := c.Get(ctx(t), key); return err },
			"Put":    func() error { return c.Put(ctx(t), key, []byte("v")) },
			"Delete": func() error { return c.Delete(ctx(t), key) },
		}
		for name, call := range calls {
			if code := status.Code(call()); code != codes.InvalidArgument {
				t.Errorf("%s with %s key: code %v, want InvalidArgument", name, keyName, code)
			}
		}
	}
}

func TestMaxLengthKeyIsAccepted(t *testing.T) {
	c := newClient(t)
	key := bytes.Repeat([]byte("k"), maxKeyLen)
	if err := c.Put(ctx(t), key, []byte("v")); err != nil {
		t.Fatalf("Put with %d-byte key: %v", maxKeyLen, err)
	}
	if got, err := c.Get(ctx(t), key); err != nil || string(got) != "v" {
		t.Fatalf("Get with %d-byte key = %q, %v; want %q", maxKeyLen, got, err, "v")
	}
	if err := c.Delete(ctx(t), key); err != nil {
		t.Fatalf("Delete with %d-byte key: %v", maxKeyLen, err)
	}
}

type failingEngine struct{ err error }

func (f failingEngine) Get([]byte) ([]byte, error) { return nil, f.err }
func (f failingEngine) Put(_, _ []byte) error      { return f.err }
func (f failingEngine) Delete([]byte) error        { return f.err }
func (f failingEngine) Close() error               { return nil }

func TestEngineErrorIsInternalAndHidden(t *testing.T) {
	secret := "open /var/lib/raidkv/wal-000042: no space left on device"
	c := newClientWith(t, failingEngine{err: errors.New(secret)})

	calls := map[string]func() error{
		"Get":    func() error { _, err := c.Get(ctx(t), []byte("k")); return err },
		"Put":    func() error { return c.Put(ctx(t), []byte("k"), []byte("v")) },
		"Delete": func() error { return c.Delete(ctx(t), []byte("k")) },
	}
	for name, call := range calls {
		st := status.Convert(call())
		if st.Code() != codes.Internal {
			t.Errorf("%s: code %v, want Internal", name, st.Code())
		}
		if strings.Contains(st.Message(), "wal-000042") {
			t.Errorf("%s: internal error leaked to client: %q", name, st.Message())
		}
	}
}

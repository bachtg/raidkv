package server_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	api "github.com/bachtg/raidkv/api/raidkv/v1"
	"github.com/bachtg/raidkv/internal/engine"
	"github.com/bachtg/raidkv/internal/server"
)

func dial(t *testing.T, e engine.Engine, opts ...server.Option) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	g, err := server.NewGRPCServer(e, opts...)
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
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func openWAL(t *testing.T) engine.Engine {
	t.Helper()
	e, err := engine.NewWALEngine(t.TempDir(), engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func TestBackupRejectsWritesServesReads(t *testing.T) {
	e := engine.NewMemEngine()
	if err := e.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	kv := api.NewKVServiceClient(dial(t, e, server.WithPrimary("10.0.0.1:7000")))

	_, putErr := kv.Put(ctx(t), &api.PutRequest{Key: []byte("k"), Value: []byte("x")})
	_, delErr := kv.Delete(ctx(t), &api.DeleteRequest{Key: []byte("k")})
	for name, err := range map[string]error{"Put": putErr, "Delete": delErr} {
		st := status.Convert(err)
		if st.Code() != codes.FailedPrecondition || st.Message() != "not primary: write to 10.0.0.1:7000" {
			t.Errorf("%s on backup = %v, want FailedPrecondition naming the primary", name, err)
		}
	}

	resp, err := kv.Get(ctx(t), &api.GetRequest{Key: []byte("k")})
	if err != nil || string(resp.GetValue()) != "v" {
		t.Fatalf("Get on backup = %v, %v; want v (writes rejected, value unchanged)", resp, err)
	}
}

func TestReadWAL(t *testing.T) {
	e := openWAL(t)
	for i := 1; i <= 5; i++ {
		if err := e.Put(fmt.Appendf(nil, "k%d", i), fmt.Appendf(nil, "v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Delete([]byte("k1")); err != nil { // seq 6
		t.Fatal(err)
	}
	rs := api.NewReplicationServiceClient(dial(t, e))

	resp, err := rs.ReadWAL(ctx(t), &api.ReadWALRequest{From: 5, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	recs := resp.GetRecords()
	if len(recs) != 2 {
		t.Fatalf("ReadWAL(5) = %v, want seqs 5 and 6", recs)
	}
	if r := recs[0]; r.GetSeq() != 5 || r.GetDelete() || string(r.GetKey()) != "k5" || string(r.GetValue()) != "v5" {
		t.Errorf("record 5 = %v, want Put k5=v5", r)
	}
	if r := recs[1]; r.GetSeq() != 6 || !r.GetDelete() || string(r.GetKey()) != "k1" {
		t.Errorf("record 6 = %v, want Delete k1", r)
	}

	resp, err = rs.ReadWAL(ctx(t), &api.ReadWALRequest{From: 7, Limit: 10})
	if err != nil || len(resp.GetRecords()) != 0 {
		t.Fatalf("ReadWAL(7) = %v, %v; want nothing new", resp.GetRecords(), err)
	}
}

func TestReadWALErrors(t *testing.T) {
	rs := api.NewReplicationServiceClient(dial(t, openWAL(t)))
	for _, req := range []*api.ReadWALRequest{
		{From: 0, Limit: 10},
		{From: 1, Limit: 0},
		{From: 1, Limit: 10001},
	} {
		if _, err := rs.ReadWAL(ctx(t), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("ReadWAL(%v) = %v, want InvalidArgument", req, err)
		}
	}

	mem := api.NewReplicationServiceClient(dial(t, engine.NewMemEngine()))
	if _, err := mem.ReadWAL(ctx(t), &api.ReadWALRequest{From: 1, Limit: 10}); status.Code(err) != codes.Unimplemented {
		t.Errorf("ReadWAL on mem engine = %v, want Unimplemented", err)
	}
}

// Records cut by a snapshot come back as OUT_OF_RANGE.
func TestReadWALTooOld(t *testing.T) {
	e, err := engine.NewWALEngine(t.TempDir(), engine.Options{SnapshotThreshold: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	rs := api.NewReplicationServiceClient(dial(t, e))

	// Threshold 1: every write snapshots.
	for i := 0; i < 1000; i++ {
		if err := e.Put([]byte("k"), []byte("v")); err != nil {
			t.Fatal(err)
		}
		_, err := rs.ReadWAL(ctx(t), &api.ReadWALRequest{From: 1, Limit: 10})
		if status.Code(err) == codes.OutOfRange {
			return
		}
		if err != nil {
			t.Fatalf("ReadWAL(1) = %v, want records or OutOfRange", err)
		}
	}
	t.Fatal("never got OutOfRange after 1000 writes with snapshots")
}

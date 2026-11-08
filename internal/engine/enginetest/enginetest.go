package enginetest

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/bachtg/raidkv/internal/engine"
)

type Factory func(t *testing.T) engine.Engine

func Run(t *testing.T, newEngine Factory) {
	open := func(t *testing.T) engine.Engine {
		t.Helper()
		e := newEngine(t)
		t.Cleanup(func() {
			if err := e.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		})
		return e
	}

	t.Run("PutThenGet", func(t *testing.T) { testPutThenGet(t, open(t)) })
	t.Run("GetMissing", func(t *testing.T) { testGetMissing(t, open(t)) })
	t.Run("Overwrite", func(t *testing.T) { testOverwrite(t, open(t)) })
	t.Run("Delete", func(t *testing.T) { testDelete(t, open(t)) })
	t.Run("DeleteMissing", func(t *testing.T) { testDeleteMissing(t, open(t)) })
	t.Run("EmptyValue", func(t *testing.T) { testEmptyValue(t, open(t)) })
	t.Run("BinaryKeys", func(t *testing.T) { testBinaryKeys(t, open(t)) })
	t.Run("PutCopiesInput", func(t *testing.T) { testPutCopiesInput(t, open(t)) })
	t.Run("GetReturnsCopy", func(t *testing.T) { testGetReturnsCopy(t, open(t)) })
	t.Run("Concurrent", func(t *testing.T) { testConcurrent(t, open(t)) })
	t.Run("ConcurrentSameKey", func(t *testing.T) { testConcurrentSameKey(t, open(t)) })
	t.Run("Oracle", func(t *testing.T) { testOracle(t, open(t)) })
	t.Run("Limits", func(t *testing.T) { testLimits(t, open(t)) })
}

// --- helpers ---

func mustPut(t *testing.T, e engine.Engine, k, v string) {
	t.Helper()
	if err := e.Put([]byte(k), []byte(v)); err != nil {
		t.Fatalf("Put(%q): %v", k, err)
	}
}

func mustDelete(t *testing.T, e engine.Engine, k string) {
	t.Helper()
	if err := e.Delete([]byte(k)); err != nil {
		t.Fatalf("Delete(%q): %v", k, err)
	}
}

func wantValue(t *testing.T, e engine.Engine, k, want string) {
	t.Helper()
	got, err := e.Get([]byte(k))
	if err != nil {
		t.Fatalf("Get(%q): unexpected error %v", k, err)
	}
	if string(got) != want {
		t.Fatalf("Get(%q) = %q, want %q", k, got, want)
	}
}

func wantNotFound(t *testing.T, e engine.Engine, k string) {
	t.Helper()
	got, err := e.Get([]byte(k))
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("Get(%q) = %q, %v; want ErrNotFound", k, got, err)
	}
}

// --- tests ---

func testPutThenGet(t *testing.T, e engine.Engine) {
	mustPut(t, e, "a", "1")
	mustPut(t, e, "b", "2")
	wantValue(t, e, "a", "1")
	wantValue(t, e, "b", "2")
}

func testGetMissing(t *testing.T, e engine.Engine) {
	wantNotFound(t, e, "nope")
}

func testOverwrite(t *testing.T, e engine.Engine) {
	mustPut(t, e, "k", "old")
	mustPut(t, e, "k", "new")
	wantValue(t, e, "k", "new")
}

func testDelete(t *testing.T, e engine.Engine) {
	mustPut(t, e, "k", "v")
	mustDelete(t, e, "k")
	wantNotFound(t, e, "k")
}

func testDeleteMissing(t *testing.T, e engine.Engine) {
	mustDelete(t, e, "never-existed")
}

// An empty value must not be confused with a missing key.
func testEmptyValue(t *testing.T, e engine.Engine) {
	if err := e.Put([]byte("k"), []byte{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := e.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after Put of empty value: %v (an empty value is not a missing key)", err)
	}
	if len(got) != 0 {
		t.Fatalf("Get = %q, want empty", got)
	}
}

// Keys are bytes, not text: zero bytes and invalid UTF-8 must work.
func testBinaryKeys(t *testing.T, e engine.Engine) {
	keys := [][]byte{{0x00}, {0x00, 0x00}, {0xff, 0xfe}, []byte("a\x00b")}
	for i, k := range keys {
		if err := e.Put(k, []byte{byte(i)}); err != nil {
			t.Fatalf("Put(%x): %v", k, err)
		}
	}
	for i, k := range keys {
		got, err := e.Get(k)
		if err != nil || !bytes.Equal(got, []byte{byte(i)}) {
			t.Fatalf("Get(%x) = %x, %v; want %x", k, got, err, []byte{byte(i)})
		}
	}
}

// The caller may reuse its buffers after Put returns.
func testPutCopiesInput(t *testing.T, e engine.Engine) {
	key := []byte("k")
	val := []byte("original")
	if err := e.Put(key, val); err != nil {
		t.Fatalf("Put: %v", err)
	}
	copy(val, "XXXXXXXX")
	key[0] = 'z'
	wantValue(t, e, "k", "original")
}

// The caller may modify what Get returned without touching the stored value.
func testGetReturnsCopy(t *testing.T, e engine.Engine) {
	mustPut(t, e, "k", "original")
	got, err := e.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	copy(got, "XXXXXXXX")
	wantValue(t, e, "k", "original")
}

// Many goroutines at once. Run with `go test -race` to catch data races.
func testConcurrent(t *testing.T, e engine.Engine) {
	const workers, perWorker = 8, 500
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWorker {
				k := []byte(fmt.Sprintf("w%d-k%d", w, i))
				if err := e.Put(k, k); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				if got, err := e.Get(k); err != nil || !bytes.Equal(got, k) {
					t.Errorf("Get(%s) = %q, %v", k, got, err)
					return
				}
				if i%3 == 0 {
					if err := e.Delete(k); err != nil {
						t.Errorf("Delete: %v", err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	for w := range workers {
		for i := range perWorker {
			k := fmt.Sprintf("w%d-k%d", w, i)
			if i%3 == 0 {
				wantNotFound(t, e, k)
			} else {
				wantValue(t, e, k, k)
			}
		}
	}
}

// Every writer stores a value made of one repeated byte, so a Get that sees a
// mix of two writes, or a value of the wrong length, is caught.
func testConcurrentSameKey(t *testing.T, e engine.Engine) {
	const workers, perWorker, keys, valueLen = 8, 2000, 4, 64
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value := bytes.Repeat([]byte{byte('a' + w)}, valueLen)
			for i := range perWorker {
				k := []byte(fmt.Sprintf("shared-%d", i%keys))
				var err error
				switch i % 3 {
				case 0:
					err = e.Put(k, value)
				case 1:
					err = e.Delete(k)
				default:
					var got []byte
					got, err = e.Get(k)
					if errors.Is(err, engine.ErrNotFound) {
						err = nil
					} else if err == nil && (len(got) != valueLen || !bytes.Equal(got, bytes.Repeat(got[:1], valueLen))) {
						t.Errorf("Get(%s) = %q: not a value any writer wrote", k, got)
						return
					}
				}
				if err != nil {
					t.Errorf("op on %s: %v", k, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// Keys and values at the limit are accepted; one byte more is rejected with
// ErrInvalidArgument and nothing is stored.
func testLimits(t *testing.T, e engine.Engine) {
	maxKey := bytes.Repeat([]byte("k"), engine.MaxKeyLength)
	longKey := bytes.Repeat([]byte("k"), engine.MaxKeyLength+1)
	maxValue := bytes.Repeat([]byte("v"), engine.MaxValueLength)
	longValue := bytes.Repeat([]byte("v"), engine.MaxValueLength+1)

	if err := e.Put(maxKey, maxValue); err != nil {
		t.Fatalf("Put with max-length key and value: %v", err)
	}
	if got, err := e.Get(maxKey); err != nil || !bytes.Equal(got, maxValue) {
		t.Fatalf("Get max-length key: len %d, %v; want len %d", len(got), err, len(maxValue))
	}
	if err := e.Delete(maxKey); err != nil {
		t.Fatalf("Delete max-length key: %v", err)
	}

	if err := e.Put(longKey, []byte("v")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Put with %d-byte key: %v, want ErrInvalidArgument", len(longKey), err)
	}
	if err := e.Delete(longKey); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Delete with %d-byte key: %v, want ErrInvalidArgument", len(longKey), err)
	}
	if err := e.Put([]byte("k"), longValue); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Put with %d-byte value: %v, want ErrInvalidArgument", len(longValue), err)
	}
	wantNotFound(t, e, "k")
}

func testOracle(t *testing.T, e engine.Engine) {
	const ops = 20_000
	seed := rand.Int63()
	rng := rand.New(rand.NewSource(seed))
	model := map[string]string{}
	key := func() string { return fmt.Sprintf("k%d", rng.Intn(200)) } // small keyspace: lots of overwrites

	for i := range ops {
		k := key()
		switch op := rng.Intn(10); {
		case op < 4: // 40% put
			v := fmt.Sprintf("v%d", rng.Int())
			if err := e.Put([]byte(k), []byte(v)); err != nil {
				t.Fatalf("seed %d, op %d: Put(%q): %v", seed, i, k, err)
			}
			model[k] = v
		case op < 6: // 20% delete
			if err := e.Delete([]byte(k)); err != nil {
				t.Fatalf("seed %d, op %d: Delete(%q): %v", seed, i, k, err)
			}
			delete(model, k)
		default: // 40% get
			got, err := e.Get([]byte(k))
			want, ok := model[k]
			switch {
			case ok && (err != nil || string(got) != want):
				t.Fatalf("seed %d, op %d: Get(%q) = %q, %v; want %q", seed, i, k, got, err, want)
			case !ok && !errors.Is(err, engine.ErrNotFound):
				t.Fatalf("seed %d, op %d: Get(%q) = %q, %v; want ErrNotFound", seed, i, k, got, err)
			}
		}
	}
}

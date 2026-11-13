package engine

import (
	"bytes"
	"testing"
	"testing/iotest"
)

// io.Reader may return fewer bytes than asked for. Replay must still read
// every record correctly when each Read returns a single byte.
func TestReplayShortReads(t *testing.T) {
	var log []byte
	log = append(log, encodeRecord(operationPut, []byte("a"), []byte("1"))...)
	log = append(log, encodeRecord(operationPut, []byte("b"), []byte("xyz"))...)
	log = append(log, encodeRecord(operationDel, []byte("a"), nil)...)

	store := make(map[string][]byte)
	offset, err := replayRecords(store, iotest.OneByteReader(bytes.NewReader(log)), int64(len(log)), false)
	if err != nil {
		t.Fatalf("replayRecords: %v", err)
	}
	if offset != int64(len(log)) {
		t.Fatalf("offset = %d, want %d", offset, len(log))
	}
	if _, ok := store["a"]; ok {
		t.Fatalf("a should be deleted")
	}
	if got := string(store["b"]); got != "xyz" {
		t.Fatalf("b = %q, want %q", got, "xyz")
	}
}

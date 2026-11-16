package engine

import (
	"errors"
	"testing"
)

func TestOptionsRejectInvalid(t *testing.T) {
	for name, opts := range map[string]Options{
		"negative threshold": {SnapshotThreshold: -1},
		"unknown mode":       {Sync: SyncMode(99)},
		"negative interval":  {Sync: SyncPeriodic, SyncInterval: -1},
	} {
		if _, err := NewWALEngine(t.TempDir(), opts); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: err = %v, want ErrInvalidArgument", name, err)
		}
	}
}

func TestOptionsDefaults(t *testing.T) {
	e, err := NewWALEngine(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	w := e.(*walEngine)
	if w.snapshotThreshold != DefaultSnapshotThreshold || w.syncMode != SyncGroup || w.syncInterval != DefaultSyncInterval {
		t.Fatalf("threshold %d, mode %v, interval %v; want %d, group, %v",
			w.snapshotThreshold, w.syncMode, w.syncInterval, DefaultSnapshotThreshold, DefaultSyncInterval)
	}
}

func TestParseSyncMode(t *testing.T) {
	for _, m := range []SyncMode{SyncGroup, SyncAlways, SyncPeriodic} {
		got, err := ParseSyncMode(m.String())
		if err != nil || got != m {
			t.Errorf("ParseSyncMode(%q) = %v, %v; want %v", m.String(), got, err, m)
		}
	}
	if _, err := ParseSyncMode("perodic"); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("ParseSyncMode(typo): err = %v, want ErrInvalidArgument", err)
	}
}

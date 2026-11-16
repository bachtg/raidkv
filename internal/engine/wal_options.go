package engine

import (
	"fmt"
	"time"
)

type SyncMode int

const (
	SyncGroup SyncMode = iota
	SyncAlways
	// SyncPeriodic acks before the fsync: a power loss can drop the last
	// SyncInterval of acked writes.
	SyncPeriodic
)

const (
	DefaultSnapshotThreshold = 64 << 20
	DefaultSyncInterval      = 100 * time.Millisecond
)

func (m SyncMode) String() string {
	switch m {
	case SyncGroup:
		return "group"
	case SyncAlways:
		return "always"
	case SyncPeriodic:
		return "periodic"
	default:
		return fmt.Sprintf("SyncMode(%d)", int(m))
	}
}

func ParseSyncMode(s string) (SyncMode, error) {
	for _, m := range []SyncMode{SyncGroup, SyncAlways, SyncPeriodic} {
		if m.String() == s {
			return m, nil
		}
	}
	return 0, fmt.Errorf("%w: unknown sync mode %q, want group, always or periodic", ErrInvalidArgument, s)
}

// Options configures the wal engine. Zero fields take their defaults.
type Options struct {
	SnapshotThreshold int64
	Sync              SyncMode
	SyncInterval      time.Duration
}

func (o Options) withDefaults() (Options, error) {
	if o.SnapshotThreshold < 0 {
		return o, fmt.Errorf("%w: snapshot threshold must be positive, got %d", ErrInvalidArgument, o.SnapshotThreshold)
	}
	if o.SnapshotThreshold == 0 {
		o.SnapshotThreshold = DefaultSnapshotThreshold
	}

	switch o.Sync {
	case SyncGroup, SyncAlways, SyncPeriodic:
	default:
		return o, fmt.Errorf("%w: unknown sync mode %v", ErrInvalidArgument, o.Sync)
	}

	if o.SyncInterval < 0 {
		return o, fmt.Errorf("%w: sync interval must be positive, got %v", ErrInvalidArgument, o.SyncInterval)
	}
	if o.SyncInterval == 0 {
		o.SyncInterval = DefaultSyncInterval
	}
	return o, nil
}

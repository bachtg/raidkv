// Package replica keeps a backup's engine in step with the primary's wal.
package replica

import (
	"context"
	"log/slog"
	"time"

	api "github.com/bachtg/raidkv/api/raidkv/v1"
	"github.com/bachtg/raidkv/internal/engine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Config tunes the pull loop.
type Config struct {
	// Records asked for per ReadWAL call.
	Limit int
	// Sleep after a ReadWAL that returned nothing new.
	PollInterval time.Duration
}

// Run pulls the primary's wal into eng until ctx is done or a fatal error.
func Run(ctx context.Context, primary api.ReplicationServiceClient, eng engine.Replica, cfg Config) error {
	// Log only when the primary goes down or comes back.
	failing := false

	for {
		from := eng.LastSeq() + 1
		walCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		readWALResponse, err := primary.ReadWAL(walCtx, &api.ReadWALRequest{
			From:  from,
			Limit: int32(cfg.Limit),
		})
		cancel()

		if status.Code(err) == codes.OutOfRange {
			return err
		}

		switch {
		case err != nil && ctx.Err() == nil && !failing:
			slog.Warn("read wal failed, retrying", "from", from, "err", err)
			failing = true
		case err == nil && failing:
			slog.Info("read wal ok again", "from", from)
			failing = false
		}

		for _, record := range readWALResponse.GetRecords() {
			err := eng.Apply(engine.Record{
				Seq:    record.GetSeq(),
				Delete: record.GetDelete(),
				Key:    record.GetKey(),
				Value:  record.GetValue(),
			})
			if err != nil {
				return err
			}
		}

		if err != nil || len(readWALResponse.GetRecords()) < cfg.Limit {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cfg.PollInterval):
			}
		}
	}
}

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	api "github.com/bachtg/raidkv/api/raidkv/v1"
	"github.com/bachtg/raidkv/internal/engine"
	"github.com/bachtg/raidkv/internal/replica"
	"github.com/bachtg/raidkv/internal/server"
)

const shutdownTimeout = 10 * time.Second

func main() {
	addr := flag.String("addr", "127.0.0.1:7000", "address to listen on")
	engineKind := flag.String("engine", "mem", "storage engine: mem (lost on restart) or wal (durable)")
	dataDir := flag.String("data-dir", "data", "directory for the wal engine's files")
	snapshotThreshold := flag.Int64("snapshot-threshold", engine.DefaultSnapshotThreshold, "wal size in bytes that triggers a snapshot")
	syncMode := flag.String("sync", "group", "wal fsync mode: group, always or periodic (may lose the last -sync-interval of writes on power loss)")
	syncInterval := flag.Duration("sync-interval", engine.DefaultSyncInterval, "how often -sync periodic fsyncs")
	primary := flag.String("primary", "", "address of the primary; set it to run as a backup (needs -engine wal)")
	pullLimit := flag.Int("pull-limit", 1000, "records a backup asks for per call")
	pullInterval := flag.Duration("pull-interval", 10*time.Millisecond, "how long a backup sleeps when there is nothing new")
	flag.Parse()

	mode, err := engine.ParseSyncMode(*syncMode)
	if err != nil {
		slog.Error("bad -sync flag", "err", err)
		os.Exit(2)
	}
	opts := engine.Options{SnapshotThreshold: *snapshotThreshold, Sync: mode, SyncInterval: *syncInterval}

	if *pullLimit < 1 || *pullLimit > 10000 {
		slog.Error("bad -pull-limit flag: want 1 to 10000", "got", *pullLimit)
		os.Exit(2)
	}
	pull := replica.Config{Limit: *pullLimit, PollInterval: *pullInterval}
	if err := run(*addr, *engineKind, *dataDir, opts, *primary, pull); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func run(addr, engineKind, dataDir string, opts engine.Options, primary string, pull replica.Config) error {
	eng, release, err := openEngine(engineKind, dataDir, opts)
	if err != nil {
		return err
	}
	defer func() {
		if err := release(); err != nil {
			slog.Error("close engine", "err", err)
		}
	}()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	var srvOpts []server.Option
	if primary != "" {
		srvOpts = append(srvOpts, server.WithPrimary(primary))
	}
	g, err := server.NewGRPCServer(eng, srvOpts...)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if primary != "" {
		stopPull, err := startPull(ctx, primary, eng, pull)
		if err != nil {
			return err
		}
		// Stop pulling before the engine closes.
		defer stopPull()
	}
	go func() {
		<-ctx.Done()
		stop() // a second signal kills the process
		slog.Info("shutting down")
		shutdown(g)
	}()

	slog.Info("listening", "addr", lis.Addr().String())
	if err := g.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

// openEngine returns the engine and a function that closes it and releases
// anything it holds, such as the data dir lock.
func openEngine(kind, dataDir string, opts engine.Options) (engine.Engine, func() error, error) {
	switch kind {
	case "mem":
		eng := engine.NewMemEngine()
		return eng, eng.Close, nil

	case "wal":
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			return nil, nil, fmt.Errorf("create data dir: %w", err)
		}

		// MkdirAll may have just created dataDir; make its name durable too.
		if err := engine.SyncDir(filepath.Dir(dataDir)); err != nil {
			return nil, nil, fmt.Errorf("sync parent of data dir: %w", err)
		}

		unlock, err := lockDataDir(dataDir)
		if err != nil {
			return nil, nil, err
		}

		eng, err := engine.NewWALEngine(dataDir, opts)
		if err != nil {
			_ = unlock()
			return nil, nil, fmt.Errorf("open wal in %s: %w", dataDir, err)
		}
		slog.Info("opened wal", "dir", dataDir, "sync", opts.Sync, "sync_interval", opts.SyncInterval)
		release := func() error {
			return errors.Join(eng.Close(), unlock())
		}
		return eng, release, nil

	default:
		return nil, nil, fmt.Errorf("unknown engine %q: want mem or wal", kind)
	}
}

// startPull runs the pull loop; the returned func stops it.
func startPull(ctx context.Context, primary string, eng engine.Engine, cfg replica.Config) (func(), error) {
	r, ok := eng.(engine.Replica)
	if !ok {
		return nil, errors.New("-primary needs -engine wal")
	}

	conn, err := grpc.NewClient(primary,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// One value can be up to 4 MiB.
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)),
		// The default backoff grows to 120s.
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second},
			MinConnectTimeout: time.Second,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial primary %s: %w", primary, err)
	}

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		slog.Info("pulling wal", "primary", primary, "limit", cfg.Limit, "interval", cfg.PollInterval)
		err := replica.Run(ctx, api.NewReplicationServiceClient(conn), r, cfg)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("pull loop stopped, this backup is no longer in sync", "err", err)
		}
	}()

	return func() {
		cancel()
		<-done
		_ = conn.Close()
	}, nil
}

// shutdown lets in-flight requests finish, then cuts off whatever is left
// after shutdownTimeout so a stalled client cannot keep the server alive.
func shutdown(g *grpc.Server) {
	done := make(chan struct{})
	go func() {
		g.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(shutdownTimeout):
		slog.Warn("graceful shutdown timed out, closing connections", "timeout", shutdownTimeout)
		g.Stop()
	}
}

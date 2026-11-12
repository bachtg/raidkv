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

	"github.com/bachtg/raidkv/internal/engine"
	"github.com/bachtg/raidkv/internal/server"
)

const shutdownTimeout = 10 * time.Second

func main() {
	addr := flag.String("addr", "127.0.0.1:7000", "address to listen on")
	engineKind := flag.String("engine", "mem", "storage engine: mem (lost on restart) or wal (durable)")
	dataDir := flag.String("data-dir", "data", "directory for the wal engine's files")
	flag.Parse()

	if err := run(*addr, *engineKind, *dataDir); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func run(addr, engineKind, dataDir string) error {
	eng, release, err := openEngine(engineKind, dataDir)
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

	g, err := server.NewGRPCServer(eng)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
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
func openEngine(kind, dataDir string) (engine.Engine, func() error, error) {
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

		eng, err := engine.NewWALEngine(dataDir)
		if err != nil {
			_ = unlock()
			return nil, nil, fmt.Errorf("open wal in %s: %w", dataDir, err)
		}
		slog.Info("opened wal", "dir", dataDir)
		release := func() error {
			return errors.Join(eng.Close(), unlock())
		}
		return eng, release, nil

	default:
		return nil, nil, fmt.Errorf("unknown engine %q: want mem or wal", kind)
	}
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

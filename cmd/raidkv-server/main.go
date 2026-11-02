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
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/bachtg/raidkv/internal/engine"
	"github.com/bachtg/raidkv/internal/server"
)

const shutdownTimeout = 10 * time.Second

func main() {
	addr := flag.String("addr", "127.0.0.1:7000", "address to listen on")
	flag.Parse()

	if err := run(*addr); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func run(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	eng := engine.NewMemEngine()
	defer eng.Close()

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

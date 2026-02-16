// Command osqueue runs a standalone queue broker with a gRPC server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/esnunes/osqueue/broker"
	"github.com/esnunes/osqueue/storage/local"
)

func main() {
	var (
		listenAddr         = flag.String("listen", ":9090", "gRPC listen address")
		dataDir            = flag.String("data", "./data", "storage directory for queue state")
		heartbeatTimeout   = flag.Duration("heartbeat-timeout", 30*time.Second, "heartbeat staleness timeout")
		heartbeatInterval  = flag.Duration("heartbeat-check", 5*time.Second, "heartbeat check interval")
		identity           = flag.String("identity", "", "broker identity (default: hostname:pid)")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	store, err := local.New(*dataDir)
	if err != nil {
		logger.Error("failed to create storage", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	b, err := broker.New(ctx, store, broker.Config{
		Identity:               *identity,
		HeartbeatTimeout:       *heartbeatTimeout,
		HeartbeatCheckInterval: *heartbeatInterval,
	})
	if err != nil {
		logger.Error("failed to start broker", "error", err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		logger.Error("failed to listen", "addr", *listenAddr, "error", err)
		os.Exit(1)
	}

	gs := broker.NewGRPCServer(b)

	go func() {
		<-ctx.Done()
		logger.Info("shutting down...")
		gs.GracefulStop()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := b.Shutdown(shutdownCtx); err != nil {
			logger.Error("broker shutdown error", "error", err)
		}
	}()

	logger.Info("broker started", "addr", lis.Addr().String())
	if err := gs.Serve(lis); err != nil {
		fmt.Fprintf(os.Stderr, "serve: %v\n", err)
		os.Exit(1)
	}
}

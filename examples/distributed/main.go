// Command distributed demonstrates osqueue in multi-process mode using gRPC.
// A single binary with three subcommands:
//
//	go run ./examples/distributed broker     — starts broker + gRPC server
//	go run ./examples/distributed publisher  — pushes 10 tasks via gRPC
//	go run ./examples/distributed consumer   — claims and completes tasks via gRPC
//
// Run each in a separate terminal. Start the broker first.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/esnunes/osqueue/broker"
	"github.com/esnunes/osqueue/storage/local"
	"github.com/esnunes/osqueue/worker"
)

// Task is the job payload.
type Task struct {
	ID      int    `json:"id"`
	Message string `json:"message"`
}

const (
	addr     = "localhost:9090"
	dataDir  = "./examples-data"
	numTasks = 10
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <broker|publisher|consumer>\n", os.Args[0])
		os.Exit(1)
	}

	switch os.Args[1] {
	case "broker":
		runBroker()
	case "publisher":
		runPublisher()
	case "consumer":
		runConsumer()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}

func runBroker() {
	store, err := local.New(dataDir)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	b, err := broker.New(ctx, store, broker.Config{
		HeartbeatTimeout:       30 * time.Second,
		HeartbeatCheckInterval: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	gs := broker.NewGRPCServer(b)

	go func() {
		<-ctx.Done()
		fmt.Println("[broker] shutting down...")
		gs.GracefulStop()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := b.Shutdown(shutdownCtx); err != nil {
			log.Printf("broker shutdown error: %v", err)
		}
	}()

	fmt.Printf("[broker] listening on %s\n", addr)
	if err := gs.Serve(lis); err != nil {
		log.Fatal(err)
	}
}

func runPublisher() {
	ctx := context.Background()

	w, err := worker.New(ctx, worker.Config{
		BrokerAddr: addr,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()

	for i := 1; i <= numTasks; i++ {
		task := Task{ID: i, Message: fmt.Sprintf("task-%d", i)}
		data, err := json.Marshal(task)
		if err != nil {
			log.Fatal(err)
		}

		id, err := w.Push(ctx, data)
		if err != nil {
			log.Fatalf("push error: %v", err)
		}
		fmt.Printf("[publisher] pushed task %d (job %s)\n", i, id)
	}
	fmt.Println("[publisher] done")
}

func runConsumer() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	w, err := worker.New(ctx, worker.Config{
		BrokerAddr:        addr,
		HeartbeatInterval: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()

	fmt.Println("[consumer] waiting for jobs... (ctrl-c to stop)")

	for {
		job, err := w.Claim(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("claim error: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if job == nil {
			// No jobs available, poll again.
			time.Sleep(500 * time.Millisecond)
			continue
		}

		var task Task
		if err := json.Unmarshal(job.Data, &task); err != nil {
			log.Printf("unmarshal error: %v", err)
			continue
		}

		fmt.Printf("[consumer] claimed job %s: %+v\n", job.ID, task)

		// Start automatic heartbeating while we process.
		cancelHeartbeat := w.StartHeartbeat(ctx, job.ID)

		// Simulate work.
		time.Sleep(500 * time.Millisecond)

		cancelHeartbeat()

		if err := w.Complete(ctx, job.ID); err != nil {
			log.Printf("complete error: %v", err)
			continue
		}
		fmt.Printf("[consumer] completed job %s\n", job.ID)
	}
	fmt.Println("[consumer] stopped")
}

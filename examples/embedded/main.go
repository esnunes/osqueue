// Command embedded demonstrates osqueue in single-process mode.
// Broker, publisher, and consumer all run in one process using the
// type-safe Queue[T] wrapper — no gRPC needed.
//
// Usage:
//
//	go run ./examples/embedded
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/esnunes/osqueue"
	"github.com/esnunes/osqueue/broker"
	"github.com/esnunes/osqueue/storage/local"
)

// Task is the job payload.
type Task struct {
	ID      int    `json:"id"`
	Message string `json:"message"`
}

func main() {
	// Use a temp dir for storage so each run starts fresh.
	dir, err := os.MkdirTemp("", "osqueue-embedded-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := local.New(dir)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, err := broker.New(ctx, store, broker.Config{
		HeartbeatTimeout:       10 * time.Second,
		HeartbeatCheckInterval: 2 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	q := osqueue.New[Task](b)

	const numTasks = 10
	var wg sync.WaitGroup

	// Publisher goroutine: push 10 tasks.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= numTasks; i++ {
			task := Task{ID: i, Message: fmt.Sprintf("task-%d", i)}
			id, err := q.Push(ctx, task)
			if err != nil {
				log.Printf("push error: %v", err)
				return
			}
			fmt.Printf("[publisher] pushed task %d (job %s)\n", i, id)
		}
		fmt.Println("[publisher] done")
	}()

	// Consumer goroutine: claim, heartbeat, and complete tasks.
	wg.Add(1)
	go func() {
		defer wg.Done()
		const workerID = "worker-1"
		completed := 0

		for completed < numTasks {
			job, err := q.Claim(ctx, workerID)
			if err != nil {
				log.Printf("claim error: %v", err)
				return
			}
			if job == nil {
				// No jobs available yet, back off briefly.
				time.Sleep(50 * time.Millisecond)
				continue
			}

			fmt.Printf("[consumer] claimed job %s: %+v\n", job.ID, job.Data)

			// Send a heartbeat while "processing".
			if err := q.Heartbeat(ctx, job.ID, workerID); err != nil {
				log.Printf("heartbeat error: %v", err)
			}

			// Simulate work.
			time.Sleep(100 * time.Millisecond)

			if err := q.Complete(ctx, job.ID, workerID); err != nil {
				log.Printf("complete error: %v", err)
				continue
			}

			completed++
			fmt.Printf("[consumer] completed job %s (%d/%d)\n", job.ID, completed, numTasks)
		}
		fmt.Println("[consumer] done")
	}()

	wg.Wait()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := b.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	fmt.Println("all done")
}

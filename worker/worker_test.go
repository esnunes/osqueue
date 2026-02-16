package worker

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/esnunes/osqueue/broker"
	"github.com/esnunes/osqueue/storage/local"
)

func startTestBrokerAndWorker(t *testing.T) (*Worker, *broker.Broker) {
	t.Helper()
	ctx := context.Background()

	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}

	b, err := broker.New(ctx, store, broker.Config{
		Identity:               "test-broker",
		HeartbeatTimeout:       30 * time.Second,
		HeartbeatCheckInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}

	gs := broker.NewGRPCServer(b)
	go gs.Serve(lis)

	w, err := New(ctx, Config{
		BrokerAddr:        lis.Addr().String(),
		WorkerID:          "test-worker",
		HeartbeatInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("worker.New: %v", err)
	}

	t.Cleanup(func() {
		w.Close()
		gs.GracefulStop()
		b.Shutdown(context.Background())
	})

	return w, b
}

func TestWorkerPushAndClaim(t *testing.T) {
	w, _ := startTestBrokerAndWorker(t)
	ctx := context.Background()

	id, err := w.Push(ctx, []byte(`{"task":"grpc-test"}`))
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty job ID")
	}

	job, err := w.Claim(ctx)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if job == nil {
		t.Fatal("expected a job")
	}
	if job.ID != id {
		t.Errorf("job ID = %q, want %q", job.ID, id)
	}
	if string(job.Data) != `{"task":"grpc-test"}` {
		t.Errorf("data = %q, want %q", job.Data, `{"task":"grpc-test"}`)
	}
}

func TestWorkerClaimEmpty(t *testing.T) {
	w, _ := startTestBrokerAndWorker(t)

	job, err := w.Claim(context.Background())
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if job != nil {
		t.Error("expected nil job from empty queue")
	}
}

func TestWorkerHeartbeatAndComplete(t *testing.T) {
	w, _ := startTestBrokerAndWorker(t)
	ctx := context.Background()

	id, _ := w.Push(ctx, []byte(`{"task":"lifecycle"}`))
	job, _ := w.Claim(ctx)
	if job == nil {
		t.Fatal("expected a job")
	}

	// Start heartbeat.
	cancel := w.StartHeartbeat(ctx, job.ID)
	time.Sleep(250 * time.Millisecond)
	cancel()

	// Complete the job.
	if err := w.Complete(ctx, id); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Queue should be empty.
	next, _ := w.Claim(ctx)
	if next != nil {
		t.Error("expected empty queue after completion")
	}
}

func TestWorkerMultipleWorkers(t *testing.T) {
	w1, b := startTestBrokerAndWorker(t)
	ctx := context.Background()

	// Push 3 jobs via the broker directly.
	b.Push(ctx, []byte(`{"n":1}`))
	b.Push(ctx, []byte(`{"n":2}`))
	b.Push(ctx, []byte(`{"n":3}`))

	// Claim via worker 1.
	job1, err := w1.Claim(ctx)
	if err != nil || job1 == nil {
		t.Fatalf("w1 Claim: err=%v, job=%v", err, job1)
	}

	// Claim second job via same worker (workers can hold multiple).
	job2, err := w1.Claim(ctx)
	if err != nil || job2 == nil {
		t.Fatalf("w1 second Claim: err=%v, job=%v", err, job2)
	}

	if job1.ID == job2.ID {
		t.Error("expected different jobs for sequential claims")
	}
}

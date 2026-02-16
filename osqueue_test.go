package osqueue

import (
	"context"
	"testing"
	"time"

	"github.com/esnunes/osqueue/broker"
	"github.com/esnunes/osqueue/storage/local"
)

type TestTask struct {
	Name     string `json:"name"`
	Priority int    `json:"priority"`
}

func newTestQueue[T any](t *testing.T) *Queue[T] {
	t.Helper()
	ctx := context.Background()

	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}

	b, err := broker.New(ctx, store, broker.Config{
		Identity:               "test-broker",
		HeartbeatTimeout:       30 * time.Second,
		HeartbeatCheckInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	t.Cleanup(func() { b.Shutdown(context.Background()) })

	return New[T](b)
}

func TestQueuePushAndClaim(t *testing.T) {
	q := newTestQueue[TestTask](t)
	ctx := context.Background()

	task := TestTask{Name: "build-index", Priority: 1}
	id, err := q.Push(ctx, task)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty job ID")
	}

	job, err := q.Claim(ctx, "worker-1")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if job == nil {
		t.Fatal("expected a job")
	}
	if job.ID != id {
		t.Errorf("job ID = %q, want %q", job.ID, id)
	}
	if job.Data.Name != "build-index" {
		t.Errorf("name = %q, want %q", job.Data.Name, "build-index")
	}
	if job.Data.Priority != 1 {
		t.Errorf("priority = %d, want 1", job.Data.Priority)
	}
}

func TestQueueClaimEmpty(t *testing.T) {
	q := newTestQueue[TestTask](t)

	job, err := q.Claim(context.Background(), "worker-1")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if job != nil {
		t.Error("expected nil from empty queue")
	}
}

func TestQueueRoundTrip(t *testing.T) {
	q := newTestQueue[TestTask](t)
	ctx := context.Background()

	// Full lifecycle: push -> claim -> heartbeat -> complete.
	task := TestTask{Name: "full-cycle", Priority: 5}
	id, _ := q.Push(ctx, task)

	job, _ := q.Claim(ctx, "worker-1")
	if job == nil {
		t.Fatal("expected a job")
	}

	if err := q.Heartbeat(ctx, id, "worker-1"); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	if err := q.Complete(ctx, id, "worker-1"); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Queue should be empty.
	next, _ := q.Claim(ctx, "worker-1")
	if next != nil {
		t.Error("expected empty queue after complete")
	}
}

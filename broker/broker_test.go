package broker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/esnunes/osqueue/queue"
	"github.com/esnunes/osqueue/storage/local"
)

func newTestBroker(t *testing.T) (*Broker, context.Context) {
	t.Helper()
	ctx := context.Background()
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}
	b, err := New(ctx, store, Config{
		Identity:               "test-broker",
		HeartbeatTimeout:       30 * time.Second,
		HeartbeatCheckInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { b.Shutdown(context.Background()) })
	return b, ctx
}

func TestBrokerStartup(t *testing.T) {
	b, _ := newTestBroker(t)

	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.state.Broker != "test-broker" {
		t.Errorf("broker identity = %q, want %q", b.state.Broker, "test-broker")
	}
	if b.etag == "" {
		t.Error("expected non-empty etag after startup")
	}
}

func TestBrokerStartupCreatesState(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := local.New(dir)
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}

	b, err := New(ctx, store, Config{Identity: "first-broker"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	b.Shutdown(ctx)

	// Read state directly from storage.
	obj, err := store.Read(ctx, "queue.json")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	var state queue.State
	if err := json.Unmarshal(obj.Data, &state); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	// Broker field should be cleared after shutdown.
	if state.Broker != "" {
		t.Errorf("broker = %q, want empty after shutdown", state.Broker)
	}
}

func TestPushAndClaim(t *testing.T) {
	b, ctx := newTestBroker(t)

	id, err := b.Push(ctx, []byte(`{"task":"hello"}`))
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty job ID")
	}

	result, err := b.Claim(ctx, "worker-1")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if result.Empty {
		t.Fatal("expected a job, got empty")
	}
	if result.Job.ID != id {
		t.Errorf("job ID = %q, want %q", result.Job.ID, id)
	}
	if string(result.Job.Data) != `{"task":"hello"}` {
		t.Errorf("job data = %q, want %q", result.Job.Data, `{"task":"hello"}`)
	}
}

func TestClaimEmpty(t *testing.T) {
	b, ctx := newTestBroker(t)

	result, err := b.Claim(ctx, "worker-1")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if !result.Empty {
		t.Error("expected empty result from empty queue")
	}
}

func TestClaimFIFO(t *testing.T) {
	b, ctx := newTestBroker(t)

	id1, _ := b.Push(ctx, []byte(`{"n":1}`))
	b.Push(ctx, []byte(`{"n":2}`))

	result, err := b.Claim(ctx, "worker-1")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if result.Job.ID != id1 {
		t.Errorf("expected FIFO order: got %q, want %q", result.Job.ID, id1)
	}
}

func TestHeartbeat(t *testing.T) {
	b, ctx := newTestBroker(t)

	id, _ := b.Push(ctx, []byte(`{"task":"a"}`))
	b.Claim(ctx, "worker-1")

	err := b.Heartbeat(ctx, id, "worker-1")
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
}

func TestHeartbeatWrongWorker(t *testing.T) {
	b, ctx := newTestBroker(t)

	id, _ := b.Push(ctx, []byte(`{"task":"a"}`))
	b.Claim(ctx, "worker-1")

	err := b.Heartbeat(ctx, id, "worker-2")
	if err == nil {
		t.Fatal("expected error for wrong worker heartbeat")
	}
}

func TestComplete(t *testing.T) {
	b, ctx := newTestBroker(t)

	id, _ := b.Push(ctx, []byte(`{"task":"a"}`))
	b.Claim(ctx, "worker-1")

	err := b.Complete(ctx, id, "worker-1")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Queue should be empty now.
	result, _ := b.Claim(ctx, "worker-2")
	if !result.Empty {
		t.Error("expected empty queue after completion")
	}
}

func TestCompleteWrongWorker(t *testing.T) {
	b, ctx := newTestBroker(t)

	id, _ := b.Push(ctx, []byte(`{"task":"a"}`))
	b.Claim(ctx, "worker-1")

	err := b.Complete(ctx, id, "worker-2")
	if err == nil {
		t.Fatal("expected error for wrong worker completion")
	}
}

func TestGroupCommitBatching(t *testing.T) {
	b, ctx := newTestBroker(t)

	// Push multiple jobs concurrently — they should be batched.
	errs := make(chan error, 5)
	for range 5 {
		go func() {
			_, err := b.Push(ctx, []byte(`{"task":"batch"}`))
			errs <- err
		}()
	}
	for range 5 {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent Push: %v", err)
		}
	}

	// All 5 should be claimable.
	for i := range 5 {
		result, err := b.Claim(ctx, "worker-1")
		if err != nil {
			t.Fatalf("Claim %d: %v", i, err)
		}
		if result.Empty {
			t.Fatalf("expected job at position %d", i)
		}
	}
}

func TestBrokerReplacement(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := local.New(dir)
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}

	b1, err := New(ctx, store, Config{Identity: "broker-1"})
	if err != nil {
		t.Fatalf("New broker-1: %v", err)
	}

	// Start a second broker, which takes over.
	b2, err := New(ctx, store, Config{Identity: "broker-2"})
	if err != nil {
		t.Fatalf("New broker-2: %v", err)
	}
	t.Cleanup(func() { b2.Shutdown(context.Background()) })

	// broker-1 should detect replacement on next operation.
	_, err = b1.Push(ctx, []byte(`{"task":"after-replacement"}`))
	if !errors.Is(err, ErrBrokerReplaced) {
		// It may also get a context canceled error since the broker shuts down.
		if err == nil {
			t.Fatal("expected error after broker replacement")
		}
	}

	b1.Shutdown(ctx)
}

func TestStaleJobReclamation(t *testing.T) {
	ctx := context.Background()
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}

	b, err := New(ctx, store, Config{
		Identity:               "test-broker",
		HeartbeatTimeout:       200 * time.Millisecond,
		HeartbeatCheckInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { b.Shutdown(context.Background()) })

	// Push and claim a job, then let it go stale.
	b.Push(ctx, []byte(`{"task":"stale"}`))
	b.Claim(ctx, "worker-1")

	// Wait for heartbeat timeout + check interval.
	time.Sleep(350 * time.Millisecond)

	// The job should be reclaimable by another worker.
	result, err := b.Claim(ctx, "worker-2")
	if err != nil {
		t.Fatalf("Claim after reclamation: %v", err)
	}
	if result.Empty {
		t.Fatal("expected stale job to be reclaimed and claimable")
	}
	if result.Job.WorkerID != "worker-2" {
		t.Errorf("worker = %q, want %q", result.Job.WorkerID, "worker-2")
	}
	if result.Job.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", result.Job.Attempts)
	}
}

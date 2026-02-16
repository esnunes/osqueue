package queue

import (
	"testing"
	"time"
)

func TestAddJob(t *testing.T) {
	s := &State{Broker: "test"}
	now := time.Date(2026, 2, 15, 10, 0, 0, 0, time.UTC)

	id := s.AddJob([]byte(`{"task":"hello"}`), now)

	if id == "" {
		t.Fatal("expected non-empty job ID")
	}
	if len(s.Jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(s.Jobs))
	}
	job := s.Jobs[0]
	if job.ID != id {
		t.Errorf("job ID = %q, want %q", job.ID, id)
	}
	if job.Status != StatusUnclaimed {
		t.Errorf("status = %q, want %q", job.Status, StatusUnclaimed)
	}
	if !job.CreatedAt.Equal(now) {
		t.Errorf("created_at = %v, want %v", job.CreatedAt, now)
	}
}

func TestClaimJob(t *testing.T) {
	s := &State{Broker: "test"}
	now := time.Date(2026, 2, 15, 10, 0, 0, 0, time.UTC)

	id := s.AddJob([]byte(`{"task":"a"}`), now)

	claimed, ok := s.ClaimJob("worker-1", now)
	if !ok {
		t.Fatal("expected to claim a job")
	}
	if claimed.ID != id {
		t.Errorf("claimed ID = %q, want %q", claimed.ID, id)
	}
	if claimed.Status != StatusInProgress {
		t.Errorf("status = %q, want %q", claimed.Status, StatusInProgress)
	}
	if claimed.WorkerID != "worker-1" {
		t.Errorf("worker_id = %q, want %q", claimed.WorkerID, "worker-1")
	}
	if claimed.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", claimed.Attempts)
	}
}

func TestClaimJobFIFO(t *testing.T) {
	s := &State{Broker: "test"}
	now := time.Date(2026, 2, 15, 10, 0, 0, 0, time.UTC)

	id1 := s.AddJob([]byte(`{"task":"first"}`), now)
	s.AddJob([]byte(`{"task":"second"}`), now)

	claimed, ok := s.ClaimJob("worker-1", now)
	if !ok {
		t.Fatal("expected to claim a job")
	}
	if claimed.ID != id1 {
		t.Errorf("expected FIFO order: got %q, want %q", claimed.ID, id1)
	}
}

func TestClaimJobEmpty(t *testing.T) {
	s := &State{Broker: "test"}
	now := time.Date(2026, 2, 15, 10, 0, 0, 0, time.UTC)

	_, ok := s.ClaimJob("worker-1", now)
	if ok {
		t.Fatal("expected no job to claim from empty queue")
	}
}

func TestClaimJobSkipsInProgress(t *testing.T) {
	s := &State{Broker: "test"}
	now := time.Date(2026, 2, 15, 10, 0, 0, 0, time.UTC)

	s.AddJob([]byte(`{"task":"first"}`), now)
	id2 := s.AddJob([]byte(`{"task":"second"}`), now)

	// Claim first job.
	s.ClaimJob("worker-1", now)

	// Second claim should get second job.
	claimed, ok := s.ClaimJob("worker-2", now)
	if !ok {
		t.Fatal("expected to claim second job")
	}
	if claimed.ID != id2 {
		t.Errorf("claimed ID = %q, want %q", claimed.ID, id2)
	}
}

func TestUpdateHeartbeat(t *testing.T) {
	s := &State{Broker: "test"}
	now := time.Date(2026, 2, 15, 10, 0, 0, 0, time.UTC)
	later := now.Add(5 * time.Second)

	id := s.AddJob([]byte(`{"task":"a"}`), now)
	s.ClaimJob("worker-1", now)

	ok := s.UpdateHeartbeat(id, "worker-1", later)
	if !ok {
		t.Fatal("expected heartbeat update to succeed")
	}
	if !s.Jobs[0].HeartbeatAt.Equal(later) {
		t.Errorf("heartbeat_at = %v, want %v", s.Jobs[0].HeartbeatAt, later)
	}
}

func TestUpdateHeartbeatWrongWorker(t *testing.T) {
	s := &State{Broker: "test"}
	now := time.Date(2026, 2, 15, 10, 0, 0, 0, time.UTC)

	id := s.AddJob([]byte(`{"task":"a"}`), now)
	s.ClaimJob("worker-1", now)

	ok := s.UpdateHeartbeat(id, "worker-2", now)
	if ok {
		t.Fatal("expected heartbeat update to fail for wrong worker")
	}
}

func TestCompleteJob(t *testing.T) {
	s := &State{Broker: "test"}
	now := time.Date(2026, 2, 15, 10, 0, 0, 0, time.UTC)
	later := now.Add(10 * time.Second)

	id := s.AddJob([]byte(`{"task":"a"}`), now)
	s.ClaimJob("worker-1", now)

	completed, ok := s.CompleteJob(id, "worker-1", later)
	if !ok {
		t.Fatal("expected completion to succeed")
	}
	if completed.ID != id {
		t.Errorf("completed ID = %q, want %q", completed.ID, id)
	}
	if !completed.CompletedAt.Equal(later) {
		t.Errorf("completed_at = %v, want %v", completed.CompletedAt, later)
	}
	if len(s.Jobs) != 0 {
		t.Errorf("expected job removed from state, got %d jobs", len(s.Jobs))
	}
}

func TestCompleteJobWrongWorker(t *testing.T) {
	s := &State{Broker: "test"}
	now := time.Date(2026, 2, 15, 10, 0, 0, 0, time.UTC)

	id := s.AddJob([]byte(`{"task":"a"}`), now)
	s.ClaimJob("worker-1", now)

	_, ok := s.CompleteJob(id, "worker-2", now)
	if ok {
		t.Fatal("expected completion to fail for wrong worker")
	}
}

func TestReclaimStaleJobs(t *testing.T) {
	s := &State{Broker: "test"}
	now := time.Date(2026, 2, 15, 10, 0, 0, 0, time.UTC)
	timeout := 30 * time.Second

	s.AddJob([]byte(`{"task":"stale"}`), now)
	s.AddJob([]byte(`{"task":"fresh"}`), now)

	s.ClaimJob("worker-1", now)
	s.ClaimJob("worker-2", now.Add(25*time.Second))

	// At now+31s, first job is stale, second is not.
	checkTime := now.Add(31 * time.Second)
	count := s.ReclaimStaleJobs(timeout, checkTime)

	if count != 1 {
		t.Fatalf("expected 1 reclaimed job, got %d", count)
	}

	if s.Jobs[0].Status != StatusUnclaimed {
		t.Errorf("stale job status = %q, want %q", s.Jobs[0].Status, StatusUnclaimed)
	}
	if s.Jobs[0].WorkerID != "" {
		t.Errorf("stale job worker_id = %q, want empty", s.Jobs[0].WorkerID)
	}
	if s.Jobs[1].Status != StatusInProgress {
		t.Errorf("fresh job status = %q, want %q", s.Jobs[1].Status, StatusInProgress)
	}
}

func TestReclaimStaleJobPreservesAttempts(t *testing.T) {
	s := &State{Broker: "test"}
	now := time.Date(2026, 2, 15, 10, 0, 0, 0, time.UTC)
	timeout := 30 * time.Second

	s.AddJob([]byte(`{"task":"retry"}`), now)
	s.ClaimJob("worker-1", now)

	// Reclaim and re-claim.
	s.ReclaimStaleJobs(timeout, now.Add(31*time.Second))
	claimed, ok := s.ClaimJob("worker-2", now.Add(32*time.Second))
	if !ok {
		t.Fatal("expected to reclaim job")
	}
	if claimed.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", claimed.Attempts)
	}
}

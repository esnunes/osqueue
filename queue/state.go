// Package queue defines the job data model and state operations for the
// object storage queue.
package queue

import (
	"time"

	"github.com/google/uuid"
)

// Status represents the lifecycle state of a job.
type Status string

const (
	StatusUnclaimed  Status = "unclaimed"
	StatusInProgress Status = "in_progress"
)

// Job represents a single unit of work in the queue.
type Job struct {
	ID          string    `json:"id"`
	Data        []byte    `json:"data"`
	Status      Status    `json:"status"`
	WorkerID    string    `json:"worker_id,omitempty"`
	HeartbeatAt time.Time `json:"heartbeat_at,omitzero"`
	CompletedAt time.Time `json:"completed_at,omitzero"`
	Attempts    int       `json:"attempts"`
	CreatedAt   time.Time `json:"created_at"`
}

// State represents the entire queue state as stored in object storage.
type State struct {
	Broker string `json:"broker"`
	Jobs   []Job  `json:"jobs"`
}

// AddJob appends a new unclaimed job with the given data. Returns the job ID.
func (s *State) AddJob(data []byte, now time.Time) string {
	id := uuid.New().String()
	s.Jobs = append(s.Jobs, Job{
		ID:        id,
		Data:      data,
		Status:    StatusUnclaimed,
		CreatedAt: now,
	})
	return id
}

// ClaimJob assigns the first unclaimed job to the given worker.
// Returns the claimed job and true, or an empty Job and false if no jobs are available.
func (s *State) ClaimJob(workerID string, now time.Time) (Job, bool) {
	for i := range s.Jobs {
		if s.Jobs[i].Status == StatusUnclaimed {
			s.Jobs[i].Status = StatusInProgress
			s.Jobs[i].WorkerID = workerID
			s.Jobs[i].HeartbeatAt = now
			s.Jobs[i].Attempts++
			return s.Jobs[i], true
		}
	}
	return Job{}, false
}

// UpdateHeartbeat updates the heartbeat timestamp for the given job and worker.
// Returns true if the job was found and updated.
func (s *State) UpdateHeartbeat(jobID, workerID string, now time.Time) bool {
	for i := range s.Jobs {
		if s.Jobs[i].ID == jobID && s.Jobs[i].WorkerID == workerID && s.Jobs[i].Status == StatusInProgress {
			s.Jobs[i].HeartbeatAt = now
			return true
		}
	}
	return false
}

// CompleteJob removes a completed job from the state and returns it.
// Returns the completed job and true, or an empty Job and false if not found.
func (s *State) CompleteJob(jobID, workerID string, now time.Time) (Job, bool) {
	for i := range s.Jobs {
		if s.Jobs[i].ID == jobID && s.Jobs[i].WorkerID == workerID && s.Jobs[i].Status == StatusInProgress {
			job := s.Jobs[i]
			job.CompletedAt = now
			s.Jobs = append(s.Jobs[:i], s.Jobs[i+1:]...)
			return job, true
		}
	}
	return Job{}, false
}

// ReclaimStaleJobs marks in-progress jobs with heartbeats older than the
// timeout as unclaimed so they can be picked up by other workers.
// Returns the number of reclaimed jobs.
func (s *State) ReclaimStaleJobs(timeout time.Duration, now time.Time) int {
	count := 0
	for i := range s.Jobs {
		if s.Jobs[i].Status == StatusInProgress && now.Sub(s.Jobs[i].HeartbeatAt) > timeout {
			s.Jobs[i].Status = StatusUnclaimed
			s.Jobs[i].WorkerID = ""
			s.Jobs[i].HeartbeatAt = time.Time{}
			count++
		}
	}
	return count
}

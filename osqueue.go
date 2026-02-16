// Package osqueue provides a type-safe generic wrapper over the object storage
// queue broker. Queue[T] handles JSON marshaling for push and consume operations.
package osqueue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/esnunes/osqueue/broker"
)

// Job represents a claimed job with a typed payload.
type Job[T any] struct {
	ID   string
	Data T
}

// Queue provides type-safe push and claim operations over a broker.
// T is the job payload type, which must be JSON-serializable.
type Queue[T any] struct {
	broker *broker.Broker
}

// New creates a new typed queue backed by the given broker.
func New[T any](b *broker.Broker) *Queue[T] {
	return &Queue[T]{broker: b}
}

// Push marshals item as JSON and pushes it to the queue.
// Returns the job ID.
func (q *Queue[T]) Push(ctx context.Context, item T) (string, error) {
	data, err := json.Marshal(item)
	if err != nil {
		return "", fmt.Errorf("osqueue: marshal: %w", err)
	}
	return q.broker.Push(ctx, data)
}

// Claim requests the next available job from the queue and unmarshals its
// payload into T. Returns nil if no jobs are available.
func (q *Queue[T]) Claim(ctx context.Context, workerID string) (*Job[T], error) {
	result, err := q.broker.Claim(ctx, workerID)
	if err != nil {
		return nil, err
	}
	if result.Empty {
		return nil, nil
	}
	var item T
	if err := json.Unmarshal(result.Job.Data, &item); err != nil {
		return nil, fmt.Errorf("osqueue: unmarshal: %w", err)
	}
	return &Job[T]{ID: result.Job.ID, Data: item}, nil
}

// Heartbeat sends a heartbeat for the given job.
func (q *Queue[T]) Heartbeat(ctx context.Context, jobID, workerID string) error {
	return q.broker.Heartbeat(ctx, jobID, workerID)
}

// Complete marks a job as completed.
func (q *Queue[T]) Complete(ctx context.Context, jobID, workerID string) error {
	return q.broker.Complete(ctx, jobID, workerID)
}

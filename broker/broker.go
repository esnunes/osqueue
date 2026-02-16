// Package broker implements the stateless queue broker with group commit,
// heartbeat monitoring, and job lifecycle management.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/esnunes/osqueue/queue"
	"github.com/esnunes/osqueue/storage"
)

// ErrBrokerReplaced is returned when the broker detects it has been replaced
// by another broker instance via CAS.
var ErrBrokerReplaced = errors.New("osqueue: broker replaced by another instance")

// Broker mediates all interactions with the queue state in object storage.
// It batches operations via a group commit loop and monitors worker heartbeats.
type Broker struct {
	store storage.Storage
	cfg   Config

	// State protected by mu.
	mu    sync.RWMutex
	state queue.State
	etag  string

	// ops is the channel through which API calls send operations to the
	// commit loop goroutine.
	ops    chan op
	cancel context.CancelFunc
	done   chan struct{}
}

// New creates and starts a new Broker. The broker reads or creates the queue
// state in storage, registers itself, and starts the group commit loop and
// heartbeat monitor.
func New(ctx context.Context, store storage.Storage, cfg Config) (*Broker, error) {
	cfg.setDefaults()
	if cfg.Identity == "" {
		hostname, _ := os.Hostname()
		cfg.Identity = fmt.Sprintf("%s:%d", hostname, os.Getpid())
	}

	b := &Broker{
		store: store,
		cfg:   cfg,
		ops:   make(chan op, 256),
		done:  make(chan struct{}),
	}

	if err := b.initialize(ctx); err != nil {
		return nil, fmt.Errorf("broker init: %w", err)
	}

	ctx, b.cancel = context.WithCancel(ctx)

	go func() {
		defer close(b.done)
		b.commitLoop(ctx)
	}()
	go b.heartbeatLoop(ctx)

	return b, nil
}

// initialize reads or creates the queue state and registers this broker.
func (b *Broker) initialize(ctx context.Context) error {
	obj, err := b.store.Read(ctx, b.cfg.QueueKey)
	if errors.Is(err, storage.ErrNotFound) {
		// First broker: create initial state.
		state := queue.State{Broker: b.cfg.Identity}
		data, err := json.Marshal(&state)
		if err != nil {
			return err
		}
		etag, err := b.store.WriteIfMatch(ctx, b.cfg.QueueKey, data, "")
		if err != nil {
			return fmt.Errorf("create initial state: %w", err)
		}
		b.state = state
		b.etag = etag
		return nil
	}
	if err != nil {
		return err
	}

	var state queue.State
	if err := json.Unmarshal(obj.Data, &state); err != nil {
		return fmt.Errorf("unmarshal state: %w", err)
	}

	// Register this broker via CAS.
	state.Broker = b.cfg.Identity
	data, err := json.Marshal(&state)
	if err != nil {
		return err
	}
	etag, err := b.store.WriteIfMatch(ctx, b.cfg.QueueKey, data, obj.ETag)
	if err != nil {
		return fmt.Errorf("register broker: %w", err)
	}
	b.state = state
	b.etag = etag
	return nil
}

// Push adds a job to the queue. Blocks until the CAS write succeeds.
// Returns the job ID.
func (b *Broker) Push(ctx context.Context, data []byte) (string, error) {
	result, err := b.submit(ctx, func(s *queue.State, now time.Time) (any, error) {
		id := s.AddJob(data, now)
		return id, nil
	})
	if err != nil {
		return "", err
	}
	return result.(string), nil
}

// ClaimResult holds the result of a Claim operation.
type ClaimResult struct {
	Job   queue.Job
	Empty bool
}

// Claim assigns the first unclaimed job to the given worker.
// Blocks until the CAS write succeeds. Returns ClaimResult with Empty=true
// if no jobs are available.
func (b *Broker) Claim(ctx context.Context, workerID string) (ClaimResult, error) {
	result, err := b.submit(ctx, func(s *queue.State, now time.Time) (any, error) {
		job, ok := s.ClaimJob(workerID, now)
		if !ok {
			return ClaimResult{Empty: true}, nil
		}
		return ClaimResult{Job: job}, nil
	})
	if err != nil {
		return ClaimResult{}, err
	}
	return result.(ClaimResult), nil
}

// Heartbeat updates the heartbeat timestamp for a job.
func (b *Broker) Heartbeat(ctx context.Context, jobID, workerID string) error {
	_, err := b.submit(ctx, func(s *queue.State, now time.Time) (any, error) {
		if !s.UpdateHeartbeat(jobID, workerID, now) {
			return nil, fmt.Errorf("job %s not found for worker %s", jobID, workerID)
		}
		return nil, nil
	})
	return err
}

// Complete marks a job as completed and archives it.
func (b *Broker) Complete(ctx context.Context, jobID, workerID string) error {
	_, err := b.submit(ctx, func(s *queue.State, now time.Time) (any, error) {
		job, ok := s.CompleteJob(jobID, workerID, now)
		if !ok {
			return nil, fmt.Errorf("job %s not found for worker %s", jobID, workerID)
		}
		// Archive in background — don't block the CAS write.
		go b.archiveJob(context.WithoutCancel(ctx), job)
		return nil, nil
	})
	return err
}

// submit sends an operation to the commit loop and waits for the result.
func (b *Broker) submit(ctx context.Context, apply func(*queue.State, time.Time) (any, error)) (any, error) {
	o := op{
		apply:  apply,
		result: make(chan opResult, 1),
	}
	select {
	case b.ops <- o:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case r := <-o.result:
		return r.value, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Shutdown gracefully stops the broker. It drains pending operations,
// flushes the buffer, and clears the broker address from the state.
func (b *Broker) Shutdown(ctx context.Context) error {
	b.cancel()
	<-b.done

	// Clear broker identity from state.
	b.mu.Lock()
	b.state.Broker = ""
	data, err := json.Marshal(&b.state)
	b.mu.Unlock()
	if err != nil {
		return err
	}

	_, err = b.store.WriteIfMatch(ctx, b.cfg.QueueKey, data, b.etag)
	if err != nil && !errors.Is(err, storage.ErrConflict) {
		return fmt.Errorf("clear broker on shutdown: %w", err)
	}
	return nil
}

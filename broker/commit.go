package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/esnunes/osqueue/queue"
	"github.com/esnunes/osqueue/storage"
)

// op represents a buffered operation sent to the commit loop.
type op struct {
	apply  func(s *queue.State, now time.Time) (any, error)
	result chan opResult
}

type opResult struct {
	value any
	err   error
}

// commitLoop runs the group commit state machine. It owns all state mutations
// and serializes access via the ops channel.
//
// State machine:
//
//	Idle (parked, waiting for first op)
//	  -> Collecting (ops arriving)
//	     -> Flushing (CAS write in flight, new ops buffer for next cycle)
//	        -> SUCCESS: ack clients, return to Idle/Collecting
//	        -> FAIL (etag mismatch): re-read, check broker, retry or shut down
//	        -> FAIL (storage error): retry with backoff
func (b *Broker) commitLoop(ctx context.Context) {
	for {
		// Idle: wait for the first operation or shutdown.
		var batch []op
		select {
		case <-ctx.Done():
			return
		case o := <-b.ops:
			batch = append(batch, o)
		}

		// Collecting: drain any additional buffered ops.
		batch = b.drainPending(batch)

		// Flushing: apply ops to state and CAS write.
		b.flush(ctx, batch)
	}
}

// drainPending collects all currently buffered operations without blocking.
func (b *Broker) drainPending(batch []op) []op {
	for {
		select {
		case o := <-b.ops:
			batch = append(batch, o)
		default:
			return batch
		}
	}
}

// flush applies a batch of operations to the current state and attempts to
// write via CAS. On conflict, it re-reads and retries. On broker replacement,
// it signals shutdown.
func (b *Broker) flush(ctx context.Context, batch []op) {
	now := time.Now()

	for attempt := range b.cfg.MaxCASRetries {
		// Apply all ops to a copy of the state.
		state := b.cloneState()
		var results []opResult
		for _, o := range batch {
			val, err := o.apply(&state, now)
			results = append(results, opResult{value: val, err: err})
		}

		// Serialize and CAS write.
		data, err := json.Marshal(&state)
		if err != nil {
			b.ackAll(batch, results, fmt.Errorf("marshal state: %w", err))
			return
		}

		newETag, err := b.store.WriteIfMatch(ctx, b.cfg.QueueKey, data, b.etag)
		if err == nil {
			// Success: update local state and ack all clients.
			b.mu.Lock()
			b.state = state
			b.etag = newETag
			b.mu.Unlock()
			b.ackAll(batch, results, nil)
			return
		}

		if !errors.Is(err, storage.ErrConflict) {
			// Storage error (not a conflict). Retry with backoff.
			if attempt < b.cfg.MaxCASRetries-1 {
				select {
				case <-ctx.Done():
					b.ackAll(batch, nil, ctx.Err())
					return
				case <-time.After(backoff(attempt)):
				}
				if err := b.reload(ctx); err != nil {
					b.ackAll(batch, nil, fmt.Errorf("reload after storage error: %w", err))
					return
				}
				continue
			}
			b.ackAll(batch, nil, fmt.Errorf("storage error after %d retries: %w", b.cfg.MaxCASRetries, err))
			return
		}

		// CAS conflict: re-read state and check if we've been replaced.
		if err := b.reload(ctx); err != nil {
			b.ackAll(batch, nil, fmt.Errorf("reload after conflict: %w", err))
			return
		}

		b.mu.RLock()
		replaced := b.state.Broker != b.cfg.Identity
		b.mu.RUnlock()

		if replaced {
			b.ackAll(batch, nil, ErrBrokerReplaced)
			b.cancel()
			return
		}

		// Not replaced — retry with updated state.
		if attempt < b.cfg.MaxCASRetries-1 {
			continue
		}
		b.ackAll(batch, nil, fmt.Errorf("CAS conflict after %d retries", b.cfg.MaxCASRetries))
	}
}

// reload re-reads the queue state from storage and updates the local copy.
func (b *Broker) reload(ctx context.Context) error {
	obj, err := b.store.Read(ctx, b.cfg.QueueKey)
	if err != nil {
		return err
	}
	var state queue.State
	if err := json.Unmarshal(obj.Data, &state); err != nil {
		return fmt.Errorf("unmarshal state: %w", err)
	}
	b.mu.Lock()
	b.state = state
	b.etag = obj.ETag
	b.mu.Unlock()
	return nil
}

// cloneState returns a deep copy of the current state.
func (b *Broker) cloneState() queue.State {
	b.mu.RLock()
	defer b.mu.RUnlock()

	s := queue.State{
		Broker: b.state.Broker,
		Jobs:   make([]queue.Job, len(b.state.Jobs)),
	}
	copy(s.Jobs, b.state.Jobs)
	return s
}

// ackAll sends results to all waiting clients. If overrideErr is non-nil,
// all clients receive that error instead of their individual results.
func (b *Broker) ackAll(batch []op, results []opResult, overrideErr error) {
	for i, o := range batch {
		if overrideErr != nil {
			o.result <- opResult{err: overrideErr}
		} else {
			o.result <- results[i]
		}
	}
}

func backoff(attempt int) time.Duration {
	d := 50 * time.Millisecond
	for range attempt {
		d *= 2
	}
	return d
}

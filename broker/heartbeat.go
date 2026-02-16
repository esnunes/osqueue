package broker

import (
	"context"
	"time"

	"github.com/esnunes/osqueue/queue"
)

// heartbeatLoop periodically checks for stale jobs and reclaims them.
func (b *Broker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(b.cfg.HeartbeatCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.reclaimStaleJobs(ctx)
		}
	}
}

// reclaimStaleJobs submits a reclaim operation to the commit loop.
func (b *Broker) reclaimStaleJobs(ctx context.Context) {
	_, _ = b.submit(ctx, func(s *queue.State, now time.Time) (any, error) {
		s.ReclaimStaleJobs(b.cfg.HeartbeatTimeout, now)
		return nil, nil
	})
}

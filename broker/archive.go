package broker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/esnunes/osqueue/queue"
)

// archiveJob writes a completed job to a time-bucketed archive object.
// Archive writes are best-effort: failures are logged but do not block
// the queue. The job has already been removed from the queue state.
func (b *Broker) archiveJob(ctx context.Context, job queue.Job) {
	key := b.archiveKey(job)

	// Read existing archive (may not exist yet).
	var jobs []queue.Job
	var etag string
	obj, err := b.store.Read(ctx, key)
	if err == nil {
		_ = json.Unmarshal(obj.Data, &jobs)
		etag = obj.ETag
	}

	jobs = append(jobs, job)
	data, err := json.Marshal(jobs)
	if err != nil {
		return
	}

	_, _ = b.store.WriteIfMatch(ctx, key, data, etag)
}

// archiveKey returns the storage key for the archive bucket containing this job.
// Format: {prefix}YYYY-MM-DDTHH.json (hourly UTC buckets).
func (b *Broker) archiveKey(job queue.Job) string {
	t := job.CompletedAt.UTC()
	return fmt.Sprintf("%s%s.json", b.cfg.ArchiveKeyPrefix, t.Format("2006-01-02T15"))
}

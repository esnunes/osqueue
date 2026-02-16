package broker

import "time"

// Config holds the broker configuration.
type Config struct {
	// Identity is a unique identifier for this broker instance.
	// Used in the queue state to detect broker replacement.
	// Defaults to hostname:pid if empty.
	Identity string

	// QueueKey is the storage key for the queue state object.
	// Defaults to "queue.json".
	QueueKey string

	// ArchiveKeyPrefix is the storage key prefix for archive objects.
	// Defaults to "archive/".
	ArchiveKeyPrefix string

	// HeartbeatTimeout is how long a job can go without a heartbeat
	// before being reclaimed. Defaults to 30s.
	HeartbeatTimeout time.Duration

	// HeartbeatCheckInterval is how often the broker checks for stale jobs.
	// Defaults to 5s.
	HeartbeatCheckInterval time.Duration

	// MaxCASRetries is the maximum number of CAS retries before the broker
	// checks if it has been replaced. Defaults to 3.
	MaxCASRetries int
}

func (c *Config) setDefaults() {
	if c.QueueKey == "" {
		c.QueueKey = "queue.json"
	}
	if c.ArchiveKeyPrefix == "" {
		c.ArchiveKeyPrefix = "archive/"
	}
	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = 30 * time.Second
	}
	if c.HeartbeatCheckInterval == 0 {
		c.HeartbeatCheckInterval = 5 * time.Second
	}
	if c.MaxCASRetries == 0 {
		c.MaxCASRetries = 3
	}
}

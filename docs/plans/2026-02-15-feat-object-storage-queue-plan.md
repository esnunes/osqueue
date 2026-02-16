---
title: "feat: Implement Object Storage Queue"
type: feat
date: 2026-02-15
brainstorm: docs/brainstorms/2026-02-15-object-storage-queue-brainstorm.md
reference: https://turbopuffer.com/blog/object-storage-queue
---

# feat: Implement Object Storage Queue

## Overview

Build a Go library and CLI implementing a durable job queue backed by object storage, following the turbopuffer architecture. The entire queue state lives in a single JSON object, updated atomically via compare-and-set (CAS). A stateless broker mediates all storage interactions through a group commit loop, batching operations for throughput. Workers heartbeat to signal liveness; stale jobs are reclaimed automatically. The library supports embedded mode (direct Go API) and remote mode (gRPC).

## Problem Statement

Applications need durable job queues. Existing solutions require running dedicated infrastructure (Redis, RabbitMQ, SQS). This library uses object storage — infrastructure teams already operate — as the backing store, deriving strong consistency from a single CAS primitive. The result is a queue that is simple, inspectable (the state is a JSON file), and requires no additional infrastructure beyond what already exists.

## Technical Approach

### Architecture

Five Go packages with clear dependency flow:

```
storage  <--  queue  <--  broker  <--  worker
                            |
                         cmd/osqueue
```

- **`storage`** — CAS interface + local filesystem implementation
- **`queue`** — Job data model, state transitions, serialization
- **`broker`** — Group commit loop, heartbeat monitor, job assignment, optional gRPC server
- **`worker`** — Client for claiming/completing jobs, heartbeat sender
- **`cmd/osqueue`** — CLI binary wrapping the broker

### Core Design Decisions

**CAS retry semantics:** On CAS failure, the broker re-reads the state. If the `broker` field matches itself, it re-merges the buffer and retries (up to 3 times with exponential backoff). If the `broker` field differs, it has been replaced and shuts down. If re-read itself fails, retry with backoff.

**Acknowledgement semantics (pessimistic):** `Push()`, `Claim()`, and `Complete()` all block until the CAS write succeeds. This ensures at-least-once delivery: workers never receive a job that was not durably assigned. Latency includes the full CAS round-trip, but correctness is guaranteed.

**Group commit loop state machine:**
```
Idle (no buffered ops)
  -> Collecting (ops arriving, timer running)
     -> Flushing (CAS write in flight, new ops buffer for next cycle)
        -> SUCCESS: Ack buffered clients, return to Idle or Collecting
        -> FAIL (etag mismatch): Re-read, check broker field, retry or shut down
        -> FAIL (storage error): Retry with backoff
```

The loop is triggered by either: (a) the first operation arriving after idle, or (b) the previous CAS write completing. An idle loop parks (no spinning).

**Concurrency model (actor-based):** A single goroutine owns all state mutations. API handlers (Go or gRPC) send operations to the state goroutine via channels and receive results via response channels. This avoids mutexes and makes the state machine explicit.

**Archive write ordering:** Archive first, then CAS-remove from queue state. If archive write fails, the job stays in the queue (worker can retry). If CAS-remove fails after archive succeeds, the job appears temporarily in both places — cleaned up on the next successful CAS cycle. Jobs gain a `completedAt` timestamp to enable deduplication.

**Broker startup:** Read `queue.json`. If not found, create with `WriteIfMatch(etag=nil)` (create-if-not-exists). If found, overwrite broker address via CAS. Begin serving after successful CAS write.

**Broker shutdown (graceful):** On SIGTERM/context cancellation: (1) stop accepting new operations, (2) flush current buffer via CAS, (3) clear broker address from state via CAS, (4) close gRPC server, (5) exit. Drain timeout: 30 seconds.

**Worker claim when empty:** `Claim()` returns immediately with "no jobs" response. Workers poll on a configurable interval (default: 1 second). No long-polling for v1.

**Worker identity:** Broker-assigned UUIDs. A worker can claim multiple jobs. Heartbeats are per-job.

**Embedded mode broker field:** Uses a unique instance identifier (hostname + PID) instead of a network address.

**Default heartbeat values:** Interval = 5 seconds, staleness timeout = 30 seconds.

**Job IDs:** Broker-generated UUIDs. No client-provided idempotency keys for v1.

**Retry tracking:** Jobs carry an `attempts` counter. Incremented each time a job returns to unclaimed after heartbeat timeout. No dead-letter queue for v1, but the counter enables one later.

### Storage Interface

```go
// storage/storage.go

// Object represents the data and version of a stored object.
type Object struct {
    Data []byte
    ETag string // empty string means "object does not exist"
}

// Storage provides compare-and-set semantics over a key-value store.
type Storage interface {
    // Read returns the object at key, or ErrNotFound if it does not exist.
    Read(ctx context.Context, key string) (Object, error)

    // WriteIfMatch atomically writes data to key only if the current ETag
    // matches. An empty etag means "create only if not exists."
    // Returns the new ETag on success, or ErrConflict on mismatch.
    WriteIfMatch(ctx context.Context, key string, data []byte, etag string) (newETag string, err error)
}

var (
    ErrNotFound = errors.New("object not found")
    ErrConflict = errors.New("etag mismatch")
)
```

### Queue State Schema

```go
// queue/state.go

type State struct {
    Broker string `json:"broker"`
    Jobs   []Job  `json:"jobs"`
}

type Job struct {
    ID          string    `json:"id"`
    Data        []byte    `json:"data"`
    Status      Status    `json:"status"`
    WorkerID    string    `json:"worker_id,omitempty"`
    HeartbeatAt time.Time `json:"heartbeat_at,omitempty"`
    CompletedAt time.Time `json:"completed_at,omitempty"`
    Attempts    int       `json:"attempts"`
    CreatedAt   time.Time `json:"created_at"`
}

type Status string

const (
    StatusUnclaimed  Status = "unclaimed"
    StatusInProgress Status = "in_progress"
)
```

### gRPC Service Definition

```protobuf
// proto/osqueue/v1/queue.proto

service QueueService {
    rpc Push(PushRequest) returns (PushResponse);
    rpc Claim(ClaimRequest) returns (ClaimResponse);
    rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
    rpc Complete(CompleteRequest) returns (CompleteResponse);
}

message PushRequest {
    bytes data = 1;
}

message PushResponse {
    string job_id = 1;
}

message ClaimRequest {
    string worker_id = 1;
}

message ClaimResponse {
    string job_id = 1;
    bytes data = 2;
    bool empty = 3; // true if no jobs available
}

message HeartbeatRequest {
    string job_id = 1;
    string worker_id = 2;
}

message HeartbeatResponse {}

message CompleteRequest {
    string job_id = 1;
    string worker_id = 2;
}

message CompleteResponse {}
```

### Generic Client Wrapper

```go
// osqueue.go (top-level package)

// Queue[T] provides type-safe push/consume over the broker's []byte API.
type Queue[T any] struct {
    broker *broker.Broker // or gRPC client
}

func (q *Queue[T]) Push(ctx context.Context, item T) (string, error) {
    data, err := json.Marshal(item)
    // ...
}

func (q *Queue[T]) Claim(ctx context.Context) (Job[T], error) {
    // Claim from broker, unmarshal data into T
}
```

### Archive Format

Time-bucketed JSON arrays, keyed by hour in UTC:
- Path: `archive/2026-02-15T10.json`
- Content: JSON array of completed Job objects
- Archive writes are not CAS-protected (append via read-modify-write with best-effort retry)
- Granularity: hourly (not configurable for v1)

### Implementation Phases

#### Phase 1: Foundation — `storage` + `queue` packages

Tasks:
- [x] Define `Storage` interface with `Read`, `WriteIfMatch`, error types (`ErrNotFound`, `ErrConflict`) — `storage/storage.go`
- [x] Implement local filesystem storage using atomic file writes + file content hashing for ETags — `storage/local/local.go`
- [x] Define `State`, `Job`, `Status` types with JSON serialization — `queue/state.go`
- [x] Implement state operations: `AddJob`, `ClaimJob`, `UpdateHeartbeat`, `CompleteJob`, `ReclaimStaleJobs` — `queue/state.go`
- [x] Write tests for storage interface contract (test suite reusable by other implementations) — `storage/storagetest/storagetest.go`
- [x] Write tests for local storage implementation — `storage/local/local_test.go`
- [x] Write tests for queue state transitions — `queue/state_test.go`

Success criteria:
- Storage interface is defined and local implementation passes all contract tests
- Queue state operations handle all transitions correctly
- CAS semantics (conflict detection, create-if-not-exists) work reliably

#### Phase 2: Core — `broker` package (group commit + embedded mode)

Tasks:
- [x] Implement the group commit loop (buffer operations, CAS flush, ack clients) — `broker/commit.go`
- [x] Implement broker lifecycle (startup, state loading, address registration, graceful shutdown) — `broker/broker.go`
- [x] Implement heartbeat monitor (periodic scan for stale jobs, reclamation) — `broker/heartbeat.go`
- [x] Implement archive writer (time-bucketed objects, complete flow) — `broker/archive.go`
- [x] Expose direct Go API: `Push()`, `Claim()`, `Heartbeat()`, `Complete()` — `broker/broker.go`
- [x] Implement CAS retry logic with broker-replaced detection — `broker/commit.go`
- [x] Implement configuration (heartbeat interval/timeout, CAS retry count, storage key prefix) — `broker/config.go`
- [x] Write tests for group commit loop (mock storage, verify batching, verify CAS retry) — `broker/commit_test.go`
- [x] Write tests for broker lifecycle (startup, shutdown, failover detection) — `broker/broker_test.go`
- [x] Write tests for heartbeat monitoring and stale job reclamation — `broker/heartbeat_test.go`
- [x] Write tests for archive writer — `broker/archive_test.go`

Success criteria:
- Broker starts, loads/creates state, and serves operations via Go API
- Group commit batches multiple operations into single CAS writes
- CAS failures trigger correct behavior (retry vs. shutdown)
- Stale jobs are reclaimed after heartbeat timeout
- Completed jobs are archived to time-bucketed objects
- Graceful shutdown flushes buffer and clears broker address

#### Phase 3: Transport — gRPC server + `worker` package

Tasks:
- [x] Define protobuf service and messages — `proto/osqueue/v1/queue.proto`
- [x] Generate Go code from proto definitions
- [x] Implement gRPC server wrapping broker Go API — `broker/grpc.go`
- [x] Implement worker client (gRPC-based): claim, heartbeat sender, complete — `worker/worker.go`
- [ ] Implement worker broker discovery (deferred to v2) (read storage for broker address on connection failure) — `worker/discovery.go`
- [x] Write integration tests: worker + broker over gRPC — `worker/worker_test.go`
- [x] Write integration tests: multiple workers competing for jobs — `broker/integration_test.go`

Success criteria:
- Workers connect to broker via gRPC, claim jobs, send heartbeats, complete jobs
- Worker reconnects to new broker after failover (reads storage for new address)
- Multiple workers can compete for jobs without conflicts

#### Phase 4: Public API — `Queue[T]` wrapper + `cmd/osqueue`

Tasks:
- [x] Implement `Queue[T]` generic wrapper with JSON marshaling — `osqueue.go`
- [x] Implement `Job[T]` type for type-safe claim results — `osqueue.go`
- [x] Implement CLI binary with flags for storage path, listen address, heartbeat config — `cmd/osqueue/main.go`
- [x] Write tests for generic wrapper (marshal/unmarshal round-trip) — `osqueue_test.go`
- [ ] Write end-to-end test (deferred): CLI broker + worker library — `cmd/osqueue/e2e_test.go`
- [x] Add Go module dependencies (gRPC, protobuf) to `go.mod`

Success criteria:
- `Queue[T]` provides type-safe push/consume with automatic JSON marshaling
- CLI binary starts a broker that workers can connect to
- End-to-end test demonstrates full push -> claim -> heartbeat -> complete cycle

## Acceptance Criteria

### Functional Requirements

- [x] Push jobs and claim them in FIFO order
- [x] Workers send heartbeats; stale jobs are automatically reclaimed
- [x] Completed jobs are archived to time-bucketed storage objects
- [x] Broker supports embedded mode (direct Go API) and remote mode (gRPC)
- [x] Broker detects replacement via CAS and shuts down gracefully
- [x] CLI binary runs a standalone broker
- [x] `Queue[T]` provides type-safe generic wrapper
- [x] Storage interface is pluggable with local filesystem default

### Non-Functional Requirements

- [x] At-least-once delivery: no job is lost (may be delivered multiple times on failure)
- [x] Strong consistency via CAS: no conflicting state writes
- [x] Group commit batching: throughput scales with batch size, not storage latency
- [x] Graceful degradation: transient storage errors trigger retries, not broker abdication

## Dependencies & Prerequisites

- Go 1.25.5 (already pinned in mise.toml)
- `google.golang.org/grpc` + `google.golang.org/protobuf` (Phase 3)
- `protoc` compiler with `protoc-gen-go` and `protoc-gen-go-grpc` plugins (Phase 3)
- No external infrastructure required for development (local filesystem storage)

## Risk Analysis & Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| Queue state JSON grows too large with many in-flight jobs | Performance degradation in CAS cycle | Document known limitation. Add monitoring for state file size. Optimize serialization if needed. |
| Archive write + queue state remove is not atomic | Completed job appears in both places temporarily | Archive-first ordering + deduplication via `completedAt` timestamp. Cleanup on next CAS cycle. |
| Dual-broker window during failover | Two workers could start processing the same job | Pessimistic claims (block until CAS succeeds) eliminate this. Workers must be idempotent. |
| Poison jobs cause infinite reclaim loops | Worker resources wasted on unprocessable jobs | `attempts` counter tracks retries. Users can filter by attempt count. Dead-letter queue in v2. |

## References

- [turbopuffer: Object Storage Queue](https://turbopuffer.com/blog/object-storage-queue)
- [Brainstorm document](docs/brainstorms/2026-02-15-object-storage-queue-brainstorm.md)

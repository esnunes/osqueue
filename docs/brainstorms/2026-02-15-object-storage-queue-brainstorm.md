# Object Storage Queue — Brainstorm

**Date:** 2026-02-15
**Reference:** [turbopuffer blog: Object Storage Queue](https://turbopuffer.com/blog/object-storage-queue)

## What We're Building

A Go library and CLI that implements a durable job queue backed by object storage, following the turbopuffer architecture. The queue stores its entire state in a single JSON object on a storage backend, using compare-and-set (CAS) for atomic updates. A stateless broker mediates all storage interactions, batching writes via group commit to maximize throughput despite object storage latency.

**Core capabilities:**
- Push jobs to the queue, claim and process them with workers
- Broker handles group commit batching (buffer in memory, flush on CAS completion)
- Workers send heartbeats; stale jobs are automatically reassigned
- Completed jobs are archived to a separate storage object
- Broker supports embedded mode (in-process) or standalone mode (CLI binary)
- Direct Go API for embedded use; optional gRPC server for remote clients

## Why This Approach

The turbopuffer design is elegant because it derives strong consistency from a single primitive: compare-and-set on object storage. This eliminates the need for distributed consensus, external databases, or complex coordination protocols. The entire queue state lives in one JSON object, making it inspectable, debuggable, and recoverable.

We chose **layered packages** because:
- The storage interface is a hard requirement (pluggable backends)
- Broker and worker roles are distinct and map to separate concerns
- Clean package boundaries make the public Go API clear for library consumers
- Each layer can be tested independently against mocks

## Key Decisions

1. **Scope:** Full step-4 design (CAS + group commit + broker + HA with heartbeats)
2. **Storage:** Pluggable interface with CAS semantics. Default implementation uses local filesystem
3. **Packaging:** Go library for embedding + CLI binary (`cmd/osqueue`) for standalone broker
4. **Message type:** Job payloads are `[]byte` at the storage/gRPC layer. The Go client library provides a generic `Queue[T]` wrapper that handles JSON marshaling for type-safe push/consume
5. **Transport:** gRPC for remote broker-worker communication. In embedded mode, direct Go API calls (no network)
6. **Embedded mode:** Broker exposes a direct Go API for in-process use. gRPC server is optional — users can start it to allow remote clients alongside embedded ones
7. **Heartbeats:** Configurable intervals and timeouts (defaults TBD during planning)
8. **Completed jobs:** Archived to a separate storage object. Queue file stays lean
9. **Architecture:** Layered packages — `storage`, `queue`, `broker`, `worker`, `cmd/osqueue`

## Architecture Overview

```
┌──────────────────────────────────────────────┐
│              Application                     │
│  ┌──────────┐    ┌───────────────┐           │
│  │  Pusher  │    │    Worker     │           │
│  │ (client) │    │  (consumer)   │           │
│  └────┬─────┘    └───────┬───────┘           │
│       │ Go API / gRPC    │ Go API / gRPC     │
│  ┌────▼──────────────────▼───────┐           │
│  │          Broker               │◄── gRPC ──┤ Remote clients
│  │  - Group commit loop          │  (opt.)   │
│  │  - Heartbeat tracking         │           │
│  │  - Job state management       │           │
│  └────────────┬──────────────────┘           │
│               │  CAS read/write              │
│  ┌────────────▼──────────────────┐           │
│  │     Storage Interface         │           │
│  │  (local FS / S3 / GCS / ...) │           │
│  └───────────────────────────────┘           │
└──────────────────────────────────────────────┘
```

### Package Structure

- **`storage`** — CAS interface (`Read`, `WriteIfMatch`) + local filesystem implementation
- **`queue`** — Job data structures, state transitions (unclaimed -> in-progress -> completed), serialization
- **`broker`** — Group commit loop, gRPC server, heartbeat monitoring, job assignment
- **`worker`** — gRPC client, heartbeat sender, job execution lifecycle
- **`cmd/osqueue`** — CLI binary wrapping the broker package

### Data Flow

1. **Push:** Client calls broker (Go API or gRPC) -> broker buffers the job -> group commit flushes to storage via CAS
2. **Claim:** Worker requests job (Go API or gRPC) -> broker assigns first unclaimed job -> writes state via CAS
3. **Heartbeat:** Worker sends periodic heartbeat -> broker updates timestamp in queue state -> flushed via group commit
4. **Complete:** Worker reports completion -> broker moves job to archive object -> removes from queue state
5. **Failover:** If broker CAS write fails (another broker took over), broker discovers replacement and shuts down

### Queue State (JSON)

```json
{
  "broker": "10.0.0.42:3000",
  "jobs": [
    {"id": "abc", "data": {...}, "status": "in_progress", "heartbeat": "2026-02-15T10:00:00Z", "worker": "w1"},
    {"id": "def", "data": {...}, "status": "unclaimed"}
  ]
}
```

CAS versioning is handled by the storage layer (etag/generation), not by the application.

## Resolved Questions

1. **Archive format:** Time-bucketed objects (e.g., `archive/2026-02-15T10.json`). Prevents unbounded file growth and makes pruning easy.
2. **Job priority:** FIFO only for v1. Priority levels can be added later if needed.
3. **Max queue size:** No limit for v1. The archive mechanism prevents completed jobs from accumulating. Backpressure can be added later.

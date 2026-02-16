# Object Storage Queue

A job queue backed by object storage with compare-and-swap (CAS) semantics. All queue state lives in a single JSON object, making it easy to run on top of any storage backend that supports conditional writes (S3, GCS, local filesystem, etc.).

## How It Works

- A **broker** manages queue state through a group commit loop that batches operations into CAS writes
- **Publishers** push jobs; **consumers** claim, heartbeat, and complete them
- Jobs that miss their heartbeat deadline are automatically reclaimed
- Completed jobs are archived to time-bucketed storage keys

Two deployment modes:

- **Embedded** -- broker, publishers, and consumers share a single process using the `Queue[T]` generic wrapper
- **Distributed** -- broker runs a gRPC server; publishers and consumers connect as remote clients via the `worker` package

## Install

```
go get github.com/esnunes/osqueue
```

## Usage

### Embedded

Run the broker in-process with the type-safe generic wrapper:

```go
store, _ := local.New("/tmp/queue-data")
b, _ := broker.New(ctx, store, broker.Config{})
q := osqueue.New[Task](b)

// Push
id, _ := q.Push(ctx, Task{Name: "build-index"})

// Claim
job, _ := q.Claim(ctx, "worker-1")

// Heartbeat (keep the job alive during processing)
_ = q.Heartbeat(ctx, job.ID, "worker-1")

// Complete
_ = q.Complete(ctx, job.ID, "worker-1")

// Shutdown
_ = b.Shutdown(ctx)
```

### Distributed (gRPC)

Start a broker with the standalone binary or programmatically:

```go
store, _ := local.New("./data")
b, _ := broker.New(ctx, store, broker.Config{})
gs := broker.NewGRPCServer(b)
lis, _ := net.Listen("tcp", ":9090")
gs.Serve(lis)
```

Connect from a publisher or consumer using the worker client:

```go
w, _ := worker.New(ctx, worker.Config{
    BrokerAddr:        "localhost:9090",
    HeartbeatInterval: 5 * time.Second,
})
defer w.Close()

// Push
id, _ := w.Push(ctx, data)

// Claim
job, _ := w.Claim(ctx)

// Automatic heartbeating in the background
cancel := w.StartHeartbeat(ctx, job.ID)
// ... do work ...
cancel()

// Complete
_ = w.Complete(ctx, job.ID)
```

### Standalone Broker

```
go run ./cmd/osqueue -listen :9090 -data ./data
```

Flags: `-listen`, `-data`, `-heartbeat-timeout`, `-heartbeat-check`, `-identity`.

## Packages

| Package | Description |
|---------|-------------|
| `osqueue` | `Queue[T]` generic wrapper with JSON marshaling |
| `broker` | Broker with group commit, heartbeat monitor, gRPC server |
| `worker` | gRPC client with `StartHeartbeat()` |
| `storage` | `Storage` interface (CAS semantics) |
| `storage/local` | Local filesystem storage backend |
| `queue` | Job and State data model |

## Examples

Runnable examples in [`examples/`](examples/):

```bash
# Single-process embedded mode
go run ./examples/embedded

# Multi-process gRPC mode (run each in a separate terminal)
go run ./examples/distributed broker
go run ./examples/distributed publisher     # default 10 tasks; pass a number to override
go run ./examples/distributed consumer
```

## License

MIT

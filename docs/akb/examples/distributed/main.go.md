# examples/distributed/main.go

Demonstrates osqueue in multi-process mode using gRPC. A single binary provides three subcommands (`broker`, `publisher`, `consumer`) to run in separate terminals, showcasing distributed task queuing.

## Types

### Task

```go
type Task struct {
    ID      int    `json:"id"`
    Message string `json:"message"`
}
```

Job payload struct used for publishing and consuming tasks. Serialized to/from JSON for transport through the queue.

## Constants

### addr

```go
const addr = "localhost:9090"
```

Default gRPC listen/connect address for the broker and worker clients.

### dataDir

```go
const dataDir = "./examples-data"
```

Local filesystem directory used by the broker's local storage backend.

### defaultNumTasks

```go
const defaultNumTasks = 10
```

Default number of tasks the publisher pushes to the queue. Can be overridden by passing a number as the second argument to the `publisher` subcommand.

## Functions

### main

```go
func main()
```

Entry point that dispatches to `runBroker`, `runPublisher`, or `runConsumer` based on `os.Args[1]`. Exits with usage error if no valid subcommand is provided.

### runBroker

```go
func runBroker()
```

Starts the broker process:
1. Creates a `local.New` storage backend at `dataDir`.
2. Initializes a `broker.New` with a 30s heartbeat timeout and 5s check interval.
3. Listens on TCP at `addr` and serves via `broker.NewGRPCServer`.
4. On SIGINT, gracefully stops the gRPC server, then shuts down the broker with a 10s timeout.

**Dependencies:** `broker.New`, `broker.NewGRPCServer`, `local.New`, `net.Listen`.

### runPublisher

```go
func runPublisher()
```

Parses an optional positional argument (`os.Args[2]`) as the number of tasks to push, defaulting to `defaultNumTasks` (10). Connects to the broker via `worker.New` with `BrokerAddr: addr`, then pushes the specified number of JSON-serialized `Task` payloads using `w.Push`. Prints each pushed task's job ID. Closes the worker connection on return.

**Dependencies:** `worker.New`, `worker.Push`, `json.Marshal`.

### runConsumer

```go
func runConsumer()
```

Connects to the broker via `worker.New` with `BrokerAddr: addr` and a 5s heartbeat interval. Enters a polling loop that:
1. Calls `w.Claim` to get the next available job.
2. Deserializes the job data into a `Task`.
3. Starts automatic heartbeating via `w.StartHeartbeat` during processing.
4. Simulates 500ms of work, cancels the heartbeat, then calls `w.Complete`.
5. On no jobs available, polls again after 500ms.
6. Exits cleanly on SIGINT.

**Dependencies:** `worker.New`, `worker.Claim`, `worker.StartHeartbeat`, `worker.Complete`, `json.Unmarshal`.

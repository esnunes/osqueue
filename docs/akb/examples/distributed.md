# examples/distributed/

Demonstrates osqueue operating in distributed multi-process mode over gRPC, where a broker, publisher, and consumer run as separate processes communicating via the network. A single `main.go` binary provides three subcommands (`broker`, `publisher`, `consumer`) to illustrate the full distributed task queuing lifecycle.

## Architecture

The example follows a three-role architecture:

- **Broker** — Runs a gRPC server backed by local filesystem storage (`./examples-data`), managing job state with a 30s heartbeat timeout and 5s check interval. Handles graceful shutdown on SIGINT.
- **Publisher** — Connects to the broker over gRPC and pushes JSON-serialized `Task` payloads to the queue. Accepts an optional argument for the number of tasks (default 10).
- **Consumer** — Connects to the broker over gRPC, polls for jobs via `Claim`, processes them with automatic heartbeating, and marks them `Complete`. Polls every 500ms when idle and exits on SIGINT.

## Key Details

- **Single binary, three modes**: Subcommand dispatched via `os.Args[1]` — designed to be run in three separate terminals.
- **Default address**: `localhost:9090` for all gRPC communication.
- **Task payload**: Simple `{ID, Message}` struct serialized as JSON.
- **Core library usage**: Exercises `broker`, `worker`, and `local` storage packages — the primary public API surface of osqueue for distributed deployments.

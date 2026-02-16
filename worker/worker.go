// Package worker provides a gRPC-based client for claiming, heartbeating,
// and completing jobs from a remote broker.
package worker

import (
	"context"
	"time"

	"github.com/google/uuid"
	pb "github.com/esnunes/osqueue/proto/osqueue/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Job represents a claimed job from the queue.
type Job struct {
	ID   string
	Data []byte
}

// Config holds worker configuration.
type Config struct {
	// BrokerAddr is the gRPC address of the broker (e.g., "localhost:9090").
	BrokerAddr string

	// WorkerID is a unique identifier for this worker.
	// Defaults to a generated UUID if empty.
	WorkerID string

	// HeartbeatInterval is how often to send heartbeats for claimed jobs.
	// Defaults to 5s.
	HeartbeatInterval time.Duration

	// DialOptions are additional gRPC dial options.
	DialOptions []grpc.DialOption
}

// Worker connects to a remote broker via gRPC to claim and process jobs.
type Worker struct {
	cfg    Config
	client pb.QueueServiceClient
	conn   *grpc.ClientConn
}

// New creates a new Worker connected to the broker at cfg.BrokerAddr.
func New(ctx context.Context, cfg Config) (*Worker, error) {
	if cfg.WorkerID == "" {
		cfg.WorkerID = uuid.New().String()
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 5 * time.Second
	}

	opts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, cfg.DialOptions...)

	conn, err := grpc.NewClient(cfg.BrokerAddr, opts...)
	if err != nil {
		return nil, err
	}

	return &Worker{
		cfg:    cfg,
		client: pb.NewQueueServiceClient(conn),
		conn:   conn,
	}, nil
}

// Push sends a job to the broker. Returns the job ID.
func (w *Worker) Push(ctx context.Context, data []byte) (string, error) {
	resp, err := w.client.Push(ctx, &pb.PushRequest{Data: data})
	if err != nil {
		return "", err
	}
	return resp.JobId, nil
}

// Claim requests the next available job from the broker.
// Returns nil if no jobs are available.
func (w *Worker) Claim(ctx context.Context) (*Job, error) {
	resp, err := w.client.Claim(ctx, &pb.ClaimRequest{WorkerId: w.cfg.WorkerID})
	if err != nil {
		return nil, err
	}
	if resp.Empty {
		return nil, nil
	}
	return &Job{ID: resp.JobId, Data: resp.Data}, nil
}

// Heartbeat sends a heartbeat for the given job.
func (w *Worker) Heartbeat(ctx context.Context, jobID string) error {
	_, err := w.client.Heartbeat(ctx, &pb.HeartbeatRequest{
		JobId:    jobID,
		WorkerId: w.cfg.WorkerID,
	})
	return err
}

// Complete marks a job as completed.
func (w *Worker) Complete(ctx context.Context, jobID string) error {
	_, err := w.client.Complete(ctx, &pb.CompleteRequest{
		JobId:    jobID,
		WorkerId: w.cfg.WorkerID,
	})
	return err
}

// StartHeartbeat starts a background goroutine that sends heartbeats for the
// given job at the configured interval. Returns a cancel function to stop.
func (w *Worker) StartHeartbeat(ctx context.Context, jobID string) context.CancelFunc {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(w.cfg.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = w.Heartbeat(ctx, jobID)
			}
		}
	}()
	return cancel
}

// Close closes the gRPC connection.
func (w *Worker) Close() error {
	return w.conn.Close()
}

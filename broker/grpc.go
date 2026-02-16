package broker

import (
	"context"
	"net"

	pb "github.com/esnunes/osqueue/proto/osqueue/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCServer wraps a Broker with a gRPC server.
type GRPCServer struct {
	pb.UnimplementedQueueServiceServer
	broker *Broker
	server *grpc.Server
}

// NewGRPCServer creates a gRPC server backed by the given broker.
func NewGRPCServer(b *Broker) *GRPCServer {
	s := &GRPCServer{
		broker: b,
		server: grpc.NewServer(),
	}
	pb.RegisterQueueServiceServer(s.server, s)
	return s
}

// Serve starts the gRPC server on the given listener.
func (s *GRPCServer) Serve(lis net.Listener) error {
	return s.server.Serve(lis)
}

// GracefulStop gracefully stops the gRPC server.
func (s *GRPCServer) GracefulStop() {
	s.server.GracefulStop()
}

func (s *GRPCServer) Push(ctx context.Context, req *pb.PushRequest) (*pb.PushResponse, error) {
	id, err := s.broker.Push(ctx, req.Data)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.PushResponse{JobId: id}, nil
}

func (s *GRPCServer) Claim(ctx context.Context, req *pb.ClaimRequest) (*pb.ClaimResponse, error) {
	result, err := s.broker.Claim(ctx, req.WorkerId)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.ClaimResponse{
		JobId: result.Job.ID,
		Data:  result.Job.Data,
		Empty: result.Empty,
	}, nil
}

func (s *GRPCServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if err := s.broker.Heartbeat(ctx, req.JobId, req.WorkerId); err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.HeartbeatResponse{}, nil
}

func (s *GRPCServer) Complete(ctx context.Context, req *pb.CompleteRequest) (*pb.CompleteResponse, error) {
	if err := s.broker.Complete(ctx, req.JobId, req.WorkerId); err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.CompleteResponse{}, nil
}

func toGRPCError(err error) error {
	switch err {
	case ErrBrokerReplaced:
		return status.Error(codes.Unavailable, err.Error())
	case context.Canceled:
		return status.Error(codes.Canceled, err.Error())
	case context.DeadlineExceeded:
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

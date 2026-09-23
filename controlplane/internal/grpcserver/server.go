// Package grpcserver hosts the WorkerManager and QueryPlanner gRPC services
// that the control plane exposes to workers and the broker, respectively.
//
// M1 scope: WorkerManager logs what it received and returns zero-value
// responses. Real behavior (Redis-backed state updates) lands in M2/M3.
// QueryPlanner.ResolvePlan is implemented via internal/planner.
package grpcserver

import (
	"context"
	"log/slog"

	"github.com/andersonlaurentino/dal-controlplane/internal/planner"
	"github.com/andersonlaurentino/dal-controlplane/internal/store"
	controlplanev1 "github.com/andersonlaurentino/dal-core/proto/gen/controlplane/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	controlplanev1.UnimplementedWorkerManagerServer
	controlplanev1.UnimplementedQueryPlannerServer

	store *store.Store
	log   *slog.Logger
}

func New(st *store.Store, log *slog.Logger) *Server {
	return &Server{store: st, log: log}
}

func (s *Server) RegisterWorker(ctx context.Context, req *controlplanev1.RegisterWorkerRequest) (*controlplanev1.RegisterWorkerResponse, error) {
	s.log.Info("worker registered", "sync", req.GetSyncName(), "pid", req.GetPid())
	return &controlplanev1.RegisterWorkerResponse{}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *controlplanev1.HeartbeatRequest) (*controlplanev1.HeartbeatResponse, error) {
	s.log.Info("heartbeat",
		"sync", req.GetSyncName(),
		"phase", req.GetPhase(),
		"lag", req.GetConsumerLag(),
		"watermark", req.GetWatermark(),
	)
	return &controlplanev1.HeartbeatResponse{}, nil
}

func (s *Server) ReportError(ctx context.Context, req *controlplanev1.ReportErrorRequest) (*controlplanev1.ReportErrorResponse, error) {
	s.log.Error("worker reported error", "sync", req.GetSyncName(), "message", req.GetMessage(), "fatal", req.GetFatal())
	return &controlplanev1.ReportErrorResponse{}, nil
}

func (s *Server) ResolvePlan(ctx context.Context, req *controlplanev1.ResolvePlanRequest) (*controlplanev1.ResolvePlanResponse, error) {
	s.log.Info("resolve plan requested", "projection", req.GetProjectionName())

	plan, err := planner.Resolve(ctx, s.store, req.GetProjectionName())
	if err != nil {
		s.log.Error("resolve plan failed", "projection", req.GetProjectionName(), "err", err)
		return nil, status.Error(codes.NotFound, err.Error())
	}

	sources := make([]*controlplanev1.StorePlan, len(plan.Sources))
	for i, sp := range plan.Sources {
		sources[i] = storePlanProto(sp)
	}
	return &controlplanev1.ResolvePlanResponse{Sources: sources}, nil
}

func storePlanProto(sp planner.StorePlan) *controlplanev1.StorePlan {
	pb := &controlplanev1.StorePlan{
		StoreType:     sp.StoreType,
		ConnectionRef: sp.ConnectionRef,
		Target:        sp.Target,
		Dsn:           sp.DSN,
		Endpoint:      sp.Endpoint,
		Bucket:        sp.Bucket,
		OrderBy:       sp.OrderBy,
	}
	if sp.MinAge != nil {
		pb.HasMinAge = true
		pb.MinAgeSeconds = int64(sp.MinAge.Seconds())
	}
	if sp.MaxAge != nil {
		pb.HasMaxAge = true
		pb.MaxAgeSeconds = int64(sp.MaxAge.Seconds())
	}
	pb.Columns = make([]*controlplanev1.ColumnPlan, len(sp.Columns))
	for i, c := range sp.Columns {
		pb.Columns[i] = &controlplanev1.ColumnPlan{Source: c.Source, As: c.As}
	}
	return pb
}

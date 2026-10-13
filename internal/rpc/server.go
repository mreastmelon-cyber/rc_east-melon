package rpc

import (
	"context"
	"crypto/subtle"
	"errors"
	"log/slog"
	"strings"
	"time"

	pb "github.com/mreastmelon-cyber/right-capital-project/api/notification/v1"
	"github.com/mreastmelon-cyber/right-capital-project/internal/app"
	"github.com/mreastmelon-cyber/right-capital-project/internal/config"
	"github.com/mreastmelon-cyber/right-capital-project/internal/domain"
	"github.com/mreastmelon-cyber/right-capital-project/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type principalKey struct{}
type Principal struct {
	Source, Operator string
	Admin            bool
}
type Server struct {
	pb.UnimplementedNotificationServiceServer
	pb.UnimplementedManagementServiceServer
	App *app.Service
}

func ValidateSecrets(c config.Config, secrets transport.SecretResolver) error {
	seen := map[string]bool{}
	if len(c.Credentials) == 0 {
		return domain.ErrInvalid
	}
	for _, v := range c.Credentials {
		s, ok := secrets(v.TokenRef)
		if !ok || s == "" || seen[s] || strings.ContainsAny(s, "\r\n") {
			return domain.ErrInvalid
		}
		seen[s] = true
	}
	return nil
}
func Authenticate(ctx context.Context, method string, c config.Config, secrets transport.SecretResolver) (context.Context, error) {
	if strings.HasPrefix(method, "/grpc.health.v1.Health/") {
		return ctx, nil
	}
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return ctx, status.Error(codes.Unauthenticated, "bearer credential required")
	}
	token := strings.TrimPrefix(values[0], "Bearer ")
	var p Principal
	matched := 0
	for _, v := range c.Credentials {
		secret, ok := secrets(v.TokenRef)
		if ok && secret != "" && subtle.ConstantTimeCompare([]byte(token), []byte(secret)) == 1 {
			matched++
			p = Principal{Source: v.Source, Operator: v.Operator, Admin: v.Admin}
		}
	}
	if matched != 1 {
		return ctx, status.Error(codes.Unauthenticated, "invalid credential")
	}
	admin := strings.HasPrefix(method, "/notification.v1.ManagementService/")
	business := strings.HasPrefix(method, "/notification.v1.NotificationService/")
	if admin && !p.Admin || business && p.Admin {
		return ctx, status.Error(codes.PermissionDenied, "identity cannot access this service")
	}
	return context.WithValue(ctx, principalKey{}, p), nil
}
func New(service *app.Service, secrets transport.SecretResolver, reflectEnabled bool, options ...grpc.ServerOption) (*grpc.Server, *health.Server) {
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if recover() != nil {
				resp = nil
				err = status.Error(codes.Internal, "internal RPC failure")
			}
		}()
		ctx, err = Authenticate(ctx, info.FullMethod, service.Config, secrets)
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		resp, err = handler(ctx, req)
		if err != nil && strings.HasPrefix(info.FullMethod, "/notification.v1.ManagementService/") {
			// Record rejected management requests without payloads or credentials.
			slog.WarnContext(ctx, "management RPC rejected", "operator", principal(ctx).Operator, "method", info.FullMethod, "code", status.Code(err).String())
		}
		return resp, err
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if recover() != nil {
				err = status.Error(codes.Internal, "internal RPC failure")
			}
		}()
		ctx, err := Authenticate(ss.Context(), info.FullMethod, service.Config, secrets)
		if err != nil {
			return err
		}
		return handler(srv, &contextStream{ServerStream: ss, ctx: ctx})
	}
	options = append(options, grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(2<<20), grpc.ChainUnaryInterceptor(unary), grpc.ChainStreamInterceptor(stream))
	server := grpc.NewServer(options...)
	impl := &Server{App: service}
	pb.RegisterNotificationServiceServer(server, impl)
	pb.RegisterManagementServiceServer(server, impl)
	h := health.NewServer()
	h.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(server, h)
	if reflectEnabled {
		reflection.Register(server)
	}
	return server, h
}

type contextStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *contextStream) Context() context.Context { return s.ctx }
func principal(ctx context.Context) Principal {
	p, _ := ctx.Value(principalKey{}).(Principal)
	return p
}
func Error(e error) error {
	if e == nil {
		return nil
	}
	switch {
	case errors.Is(e, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "RPC deadline exceeded")
	case errors.Is(e, context.Canceled):
		return status.Error(codes.Canceled, "RPC canceled")
	case errors.Is(e, domain.ErrInvalid):
		return status.Error(codes.InvalidArgument, "invalid request or event contract")
	case errors.Is(e, domain.ErrConflict):
		return status.Error(codes.AlreadyExists, "identifier content conflict")
	case errors.Is(e, domain.ErrNotFound):
		return status.Error(codes.NotFound, "record not found")
	case errors.Is(e, domain.ErrPrecondition):
		return status.Error(codes.FailedPrecondition, "operation precondition not satisfied")
	case errors.Is(e, domain.ErrUnavailable):
		return status.Error(codes.Unavailable, "storage unavailable")
	default:
		return status.Error(codes.Internal, "internal service failure")
	}
}
func (s *Server) SubmitEvent(ctx context.Context, r *pb.SubmitEventRequest) (*pb.SubmitEventResponse, error) {
	if r == nil || r.Event == nil {
		return nil, Error(domain.ErrInvalid)
	}
	p := principal(ctx)
	if r.Event.Source != p.Source {
		return nil, status.Error(codes.PermissionDenied, "source does not match identity")
	}
	v, e := s.App.Submit(ctx, p.Source, r.Event)
	return v, Error(e)
}
func (s *Server) GetEvent(ctx context.Context, r *pb.GetEventRequest) (*pb.EventView, error) {
	if r == nil || r.Id == 0 {
		return nil, Error(domain.ErrInvalid)
	}
	v, tasks, e := s.App.GetEvent(ctx, r.Id, principal(ctx).Source)
	if e != nil {
		return nil, Error(e)
	}
	out := &pb.EventView{Id: v.ID, Event: &pb.Event{}}
	if e = protojson.Unmarshal(v.Payload, out.Event); e != nil {
		return nil, Error(domain.ErrUnavailable)
	}
	for _, t := range tasks {
		out.Tasks = append(out.Tasks, TaskView(t))
	}
	return out, nil
}
func (s *Server) GetTask(ctx context.Context, r *pb.GetTaskRequest) (*pb.TaskView, error) {
	if r == nil || r.Id == 0 {
		return nil, Error(domain.ErrInvalid)
	}
	t, e := s.App.GetTask(ctx, r.Id, principal(ctx).Source)
	if e != nil {
		return nil, Error(e)
	}
	return TaskView(t), nil
}
func timestamp(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}
func TaskView(t domain.Task) *pb.TaskView {
	return &pb.TaskView{Id: t.ID, EventId: t.EventID, DestinationId: t.DestinationID, State: t.State, AttemptCount: t.AttemptCount, RunNo: t.RunNo, RunAttemptCount: t.RunAttemptCount, LastErrorCode: t.LastErrorCode, LastErrorSummary: t.LastErrorSummary, NextAttemptAt: timestamp(t.NextAttemptAt), DeadlineAt: timestamppb.New(t.DeadlineAt), CompletedAt: timestamp(t.CompletedAt), ClosedAt: timestamp(t.ClosedAt)}
}
func (s *Server) ListTasks(ctx context.Context, r *pb.ListTasksRequest) (*pb.ListTasksResponse, error) {
	if r == nil {
		return nil, Error(domain.ErrInvalid)
	}
	limit, e := app.PageLimit(r.Limit)
	if e != nil || r.State != "" && !domain.ValidState(r.State) || r.DestinationId != "" && !domain.Identifier(r.DestinationId, 64) {
		return nil, Error(domain.ErrInvalid)
	}
	tasks, e := s.App.Store.ListTasks(ctx, domain.ListFilter{Destination: r.DestinationId, State: r.State, AfterID: r.AfterId, Limit: limit + 1})
	if e != nil {
		return nil, Error(e)
	}
	out := &pb.ListTasksResponse{}
	if len(tasks) > limit {
		out.NextAfterId = tasks[limit-1].ID
		tasks = tasks[:limit]
	}
	for _, t := range tasks {
		out.Tasks = append(out.Tasks, TaskView(t))
	}
	return out, nil
}
func (s *Server) ListAttempts(ctx context.Context, r *pb.ListAttemptsRequest) (*pb.ListAttemptsResponse, error) {
	if r == nil || r.TaskId == 0 {
		return nil, Error(domain.ErrInvalid)
	}
	limit, e := app.PageLimit(r.Limit)
	if e != nil {
		return nil, Error(e)
	}
	if _, e = s.App.Store.GetTask(ctx, r.TaskId); e != nil {
		return nil, Error(e)
	}
	attempts, e := s.App.Store.Attempts(ctx, r.TaskId, r.AfterId, limit+1)
	if e != nil {
		return nil, Error(e)
	}
	out := &pb.ListAttemptsResponse{}
	if len(attempts) > limit {
		out.NextAfterId = attempts[limit-1].ID
		attempts = attempts[:limit]
	}
	for _, a := range attempts {
		out.Attempts = append(out.Attempts, &pb.AttemptView{Id: a.ID, TaskId: a.TaskID, AttemptNo: a.AttemptNo, RunNo: a.RunNo, Outcome: a.Outcome, HttpStatus: uint32(a.HTTPStatus), ErrorCode: a.ErrorCode, StartedAt: timestamppb.New(a.StartedAt), FinishedAt: timestamp(a.FinishedAt), LateResultJson: string(a.LateResult)})
	}
	return out, nil
}
func (s *Server) ReplayTask(ctx context.Context, r *pb.ReplayTaskRequest) (*pb.TaskView, error) {
	t, e := s.App.Replay(ctx, principal(ctx).Operator, r)
	if e != nil {
		return nil, Error(e)
	}
	return TaskView(t), nil
}
func (s *Server) ResolveTask(ctx context.Context, r *pb.ResolveTaskRequest) (*pb.TaskView, error) {
	t, e := s.App.Resolve(ctx, principal(ctx).Operator, r)
	if e != nil {
		return nil, Error(e)
	}
	return TaskView(t), nil
}

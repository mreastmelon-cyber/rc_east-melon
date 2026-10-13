package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	pb "github.com/mreastmelon-cyber/right-capital-project/api/notification/v1"
	"github.com/mreastmelon-cyber/right-capital-project/internal/adapter"
	"github.com/mreastmelon-cyber/right-capital-project/internal/config"
	"github.com/mreastmelon-cyber/right-capital-project/internal/domain"
	"github.com/mreastmelon-cyber/right-capital-project/internal/repository"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Service struct {
	Store  *repository.Store
	Config config.Config
}

func Canonical(m proto.Message) ([]byte, [32]byte, error) {
	b, e := (protojson.MarshalOptions{EmitUnpopulated: true}).Marshal(m)
	if e != nil {
		return nil, [32]byte{}, domain.ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var v any
	if e = d.Decode(&v); e != nil {
		return nil, [32]byte{}, domain.ErrInvalid
	}
	b, e = json.Marshal(v)
	return b, sha256.Sum256(b), e
}
func ValidateEvent(e *pb.Event, source string) error {
	if e == nil || !domain.Identifier(e.EventId, 128) || !domain.Identifier(e.Source, 64) || e.Source != source || e.SchemaVersion != 1 || e.OccurredAt == nil || e.OccurredAt.CheckValid() != nil || e.OccurredAt.AsTime().Year() < 1000 {
		return domain.ErrInvalid
	}
	switch e.EventType {
	case "user.registered":
		v := e.GetRegistration()
		if v == nil || !domain.Identifier(v.UserId, 128) || !domain.Identifier(v.Channel, 64) {
			return domain.ErrInvalid
		}
	case "subscription.paid":
		v := e.GetSubscriptionPayment()
		if v == nil || !domain.Identifier(v.CustomerId, 128) || !domain.Identifier(v.SubscriptionId, 128) || v.Status != "paid" {
			return domain.ErrInvalid
		}
	case "purchase.completed":
		v := e.GetPurchase()
		if v == nil || !domain.Identifier(v.OrderId, 128) || !domain.Identifier(v.ProductId, 128) || v.Quantity == 0 {
			return domain.ErrInvalid
		}
	default:
		return domain.ErrInvalid
	}
	return nil
}
func (s *Service) Submit(ctx context.Context, source string, e *pb.Event) (*pb.SubmitEventResponse, error) {
	if err := ValidateEvent(e, source); err != nil {
		return nil, err
	}
	payload, hash, err := Canonical(e)
	if err != nil || len(payload) > 1<<20 {
		return nil, domain.ErrInvalid
	}
	old, err := s.Store.FindEvent(ctx, source, e.EventId)
	if err == nil {
		if old.HashVersion != 1 {
			return nil, domain.ErrPrecondition
		}
		if old.Hash != hash {
			return nil, domain.ErrConflict
		}
		jobs, e := s.Store.EventTasks(ctx, old.ID)
		if e != nil {
			return nil, e
		}
		return submitResponse(old.ID, jobs, true), nil
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return nil, err
	}
	channel := ""
	if v := e.GetRegistration(); v != nil {
		channel = v.Channel
	}
	targets := s.Config.Targets(source, e.EventType, e.SchemaVersion, channel)
	if len(targets) == 0 {
		return nil, domain.ErrPrecondition
	}
	tasks := make([]domain.Task, 0, len(targets))
	for _, d := range targets {
		r, err := adapter.Prepare(e, d)
		if err != nil {
			return nil, err
		}
		job := domain.Task{DestinationID: d.ID, AdapterKey: d.Adapter, ConfigVersion: s.Config.Version, Config: d, Request: r, Policy: d.Retry, IdempotencyMode: d.IdempotencyMode, IdempotencyKey: domain.UUID()}
		if d.IdempotencyMode == "provider_key" {
			job.RetentionSeconds = domain.Ptr(d.IdempotencyRetentionSeconds)
		}
		tasks = append(tasks, job)
	}
	event := domain.Event{Source: source, SourceEventID: e.EventId, Type: e.EventType, SchemaVersion: e.SchemaVersion, OccurredAt: e.OccurredAt.AsTime(), Payload: payload, Hash: hash, HashVersion: 1, ConfigVersion: s.Config.Version}
	event, jobs, duplicate, err := s.Store.CreateEvent(ctx, event, tasks)
	if err != nil {
		return nil, err
	}
	return submitResponse(event.ID, jobs, duplicate), nil
}
func submitResponse(id uint64, jobs []domain.Task, duplicate bool) *pb.SubmitEventResponse {
	r := &pb.SubmitEventResponse{Id: id, Accepted: true, Duplicate: duplicate}
	for _, t := range jobs {
		r.TaskIds = append(r.TaskIds, t.ID)
	}
	return r
}
func (s *Service) GetEvent(ctx context.Context, id uint64, source string) (domain.Event, []domain.Task, error) {
	v, e := s.Store.GetEvent(ctx, id)
	if e != nil {
		return v, nil, e
	}
	if v.Source != source {
		return v, nil, domain.ErrNotFound
	}
	tasks, e := s.Store.EventTasks(ctx, id)
	return v, tasks, e
}
func (s *Service) GetTask(ctx context.Context, id uint64, source string) (domain.Task, error) {
	t, e := s.Store.GetTask(ctx, id)
	if e != nil {
		return t, e
	}
	v, e := s.Store.GetEvent(ctx, t.EventID)
	if e != nil {
		return t, e
	}
	if v.Source != source {
		return t, domain.ErrNotFound
	}
	return t, nil
}
func operation(operator, id, reason, kind string, m proto.Message) (domain.Operation, error) {
	if !domain.Identifier(operator, 128) || !domain.Identifier(id, 128) || reason == "" || utf8.RuneCountInString(reason) > 512 {
		return domain.Operation{}, domain.ErrInvalid
	}
	b, _, e := Canonical(m)
	if e != nil {
		return domain.Operation{}, e
	}
	h := sha256.Sum256(append([]byte(kind+":"), b...))
	return domain.Operation{Operator: operator, ID: id, Type: kind, Reason: reason, Hash: h, Request: b}, nil
}
func (s *Service) Replay(ctx context.Context, operator string, r *pb.ReplayTaskRequest) (domain.Task, error) {
	if r == nil || r.TaskId == 0 || r.DeadlineSeconds > 604800 || utf8.RuneCountInString(r.VerificationEvidence) > 512 {
		return domain.Task{}, domain.ErrInvalid
	}
	op, e := operation(operator, r.OperationId, r.Reason, "replay_task", r)
	if e != nil {
		return domain.Task{}, e
	}
	return s.Store.ChangeTask(ctx, r.TaskId, op, func(t *domain.Task, now time.Time) error {
		if t.State != domain.Failed && t.State != domain.NeedsReview {
			return domain.ErrPrecondition
		}
		if (t.State == domain.NeedsReview || !t.Safe(now)) && r.VerificationEvidence == "" {
			return domain.ErrPrecondition
		}
		if t.IdempotencyMode == "provider_key" && t.AttemptCount > 0 && !t.Safe(now) {
			return domain.ErrPrecondition
		}
		if t.RunNo == ^uint32(0) {
			return domain.ErrPrecondition
		}
		seconds := int64(r.DeadlineSeconds)
		if seconds == 0 {
			seconds = t.Config.DeadlineSeconds
		}
		t.State = domain.Pending
		t.RunNo++
		t.RunAttemptCount = 0
		t.DeadlineAt = now.Add(time.Duration(seconds) * time.Second)
		t.NextAttemptAt = &now
		t.CompletedAt = nil
		t.ClosedAt = nil
		t.ClosedBy = ""
		t.LastHTTPStatus = 0
		t.LastProviderCode = ""
		t.LastErrorCode = ""
		t.LastErrorSummary = ""
		return nil
	})
}
func (s *Service) Resolve(ctx context.Context, operator string, r *pb.ResolveTaskRequest) (domain.Task, error) {
	if r == nil || r.TaskId == 0 || r.VerificationEvidence == "" || utf8.RuneCountInString(r.VerificationEvidence) > 512 {
		return domain.Task{}, domain.ErrInvalid
	}
	op, e := operation(operator, r.OperationId, r.Reason, "resolve_task", r)
	if e != nil {
		return domain.Task{}, e
	}
	return s.Store.ChangeTask(ctx, r.TaskId, op, func(t *domain.Task, now time.Time) error {
		if t.State != domain.Failed && t.State != domain.NeedsReview {
			return domain.ErrPrecondition
		}
		t.CompletedAt = &now
		t.NextAttemptAt = nil
		t.LeaseToken = ""
		t.LeaseUntil = nil
		if r.Delivered {
			t.State = domain.Succeeded
			t.ClosedAt = nil
			t.ClosedBy = ""
		} else {
			t.State = domain.Failed
			t.ClosedAt = &now
			t.ClosedBy = operator
		}
		return nil
	})
}
func PageLimit(v uint32) (int, error) {
	if v == 0 {
		return 50, nil
	}
	if v > 200 {
		return 0, fmt.Errorf("%w: page limit", domain.ErrInvalid)
	}
	return int(v), nil
}

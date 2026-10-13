//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	pb "github.com/mreastmelon-cyber/right-capital-project/api/notification/v1"
	"github.com/mreastmelon-cyber/right-capital-project/internal/app"
	"github.com/mreastmelon-cyber/right-capital-project/internal/config"
	"github.com/mreastmelon-cyber/right-capital-project/internal/domain"
	"github.com/mreastmelon-cyber/right-capital-project/internal/observability"
	"github.com/mreastmelon-cyber/right-capital-project/internal/repository"
	serviceRPC "github.com/mreastmelon-cyber/right-capital-project/internal/rpc"
	"github.com/mreastmelon-cyber/right-capital-project/internal/transport"
	"github.com/mreastmelon-cyber/right-capital-project/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func fixture(t *testing.T, ds ...domain.Destination) *app.Service {
	t.Helper()
	raw := os.Getenv("MYSQL_TEST_DSN")
	if raw == "" {
		t.Fatal("MYSQL_TEST_DSN is required for integration tests; no test is skipped")
	}
	c, e := mysql.ParseDSN(raw)
	if e != nil {
		t.Fatal("invalid test DSN")
	}
	host, _, e := net.SplitHostPort(c.Addr)
	if e != nil || c.Net != "tcp" || (host != "127.0.0.1" && host != "localhost" && host != "::1") {
		t.Fatal("integration tests only accept a loopback MySQL server")
	}
	c.DBName = ""
	admin, e := repository.Open(context.Background(), c.FormatDSN())
	if e != nil {
		t.Fatal(e)
	}
	name := "notification_it_" + strings.ReplaceAll(domain.UUID(), "-", "")
	if _, e = admin.DB.Exec("CREATE DATABASE `" + name + "`"); e != nil {
		admin.DB.Close()
		t.Fatal(e)
	}
	c.DBName = name
	store, e := repository.Open(context.Background(), c.FormatDSN())
	if e != nil {
		admin.DB.Exec("DROP DATABASE `" + name + "`")
		admin.DB.Close()
		t.Fatal(e)
	}
	t.Cleanup(func() {
		store.DB.Close()
		if _, e := admin.DB.Exec("DROP DATABASE `" + name + "`"); e != nil {
			t.Error(e)
		}
		admin.DB.Close()
	})
	if e = store.Migrate(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e = store.Migrate(context.Background()); e != nil {
		t.Fatal("migration not idempotent", e)
	}
	var absent int
	if e = store.DB.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=? AND table_name='destination_runtime_controls'", name).Scan(&absent); e != nil || absent != 0 {
		t.Fatal("removed control table exists", e)
	}
	if len(ds) == 0 {
		ds = []domain.Destination{{ID: "crm", Adapter: "crm.contact.v1", URL: "http://127.0.0.1:1/crm", AllowHTTP: true, IdempotencyMode: "natural"}}
	}
	cfg := config.Config{Version: "integration-v1", Destinations: ds, Credentials: []config.Credential{{TokenRef: "SOURCE_A", Source: "billing"}, {TokenRef: "SOURCE_B", Source: "other"}, {TokenRef: "ADMIN", Operator: "test-operator", Admin: true}}}
	var ids []string
	for _, d := range ds {
		ids = append(ids, d.ID)
	}
	cfg.Routes = []config.Route{{Source: "billing", EventType: "subscription.paid", SchemaVersion: 1, Destinations: ids}, {Source: "other", EventType: "subscription.paid", SchemaVersion: 1, Destinations: ids}}
	if e = cfg.Validate(); e != nil {
		t.Fatal(e)
	}
	return &app.Service{Store: store, Config: cfg}
}
func event(id string) *pb.Event {
	return &pb.Event{EventId: id, Source: "billing", EventType: "subscription.paid", SchemaVersion: 1, OccurredAt: timestamppb.New(time.Date(2026, 10, 13, 0, 0, 0, 123, time.UTC)), Data: &pb.Event_SubscriptionPayment{SubscriptionPayment: &pb.SubscriptionPayment{CustomerId: "customer-1", SubscriptionId: "subscription-1", Status: "paid"}}}
}
func submit(t *testing.T, s *app.Service, id string) *pb.SubmitEventResponse {
	t.Helper()
	r, e := s.Submit(context.Background(), "billing", event(id))
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func claim(t *testing.T, s *app.Service) domain.Task {
	t.Helper()
	v, e := s.Store.Claim(context.Background(), "test-worker", 45*time.Second)
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func assertState(t *testing.T, s *app.Service, id uint64, want string) domain.Task {
	t.Helper()
	v, e := s.Store.GetTask(context.Background(), id)
	if e != nil || v.State != want {
		t.Fatalf("task %d got %s want %s (%v)", id, v.State, want, e)
	}
	return v
}
func ready(t *testing.T, s *app.Service, id uint64) {
	t.Helper()
	if _, e := s.Store.DB.Exec("UPDATE notification_tasks SET next_attempt_at=UTC_TIMESTAMP(6) WHERE id=? AND state='retry_wait'", id); e != nil {
		t.Fatal(e)
	}
}
func TestConcurrentDedupAndAtomicFanout(t *testing.T) {
	s := fixture(t, domain.Destination{ID: "crm-a", Adapter: "crm.contact.v1", URL: "http://127.0.0.1:1", AllowHTTP: true}, domain.Destination{ID: "crm-b", Adapter: "crm.contact.v1", URL: "http://127.0.0.1:1", AllowHTTP: true})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first uint64
	var created int
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := s.Submit(context.Background(), "billing", event("same"))
			if e != nil {
				t.Error(e)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if first == 0 {
				first = r.Id
			}
			if r.Id != first || len(r.TaskIds) != 2 {
				t.Error("dedup or fanout failed")
			}
			if !r.Duplicate {
				created++
			}
		}()
	}
	wg.Wait()
	if created != 1 {
		t.Fatalf("created %d events", created)
	}
	changed := event("same")
	changed.GetSubscriptionPayment().CustomerId = "changed"
	if _, e := s.Submit(context.Background(), "billing", changed); !errors.Is(e, domain.ErrConflict) {
		t.Fatal("content conflict not detected", e)
	}
	var count int
	if e := s.Store.DB.QueryRow("SELECT COUNT(*) FROM notification_tasks").Scan(&count); e != nil || count != 2 {
		t.Fatal("duplicate tasks created", e, count)
	}
	s.Config.Destinations = append(s.Config.Destinations, domain.Destination{ID: "invalid", Adapter: "ad.registration.v1", URL: "http://127.0.0.1:1"})
	s.Config.Routes[0].Destinations = append(s.Config.Routes[0].Destinations, "invalid")
	if _, e := s.Submit(context.Background(), "billing", event("bad-fanout")); !errors.Is(e, domain.ErrInvalid) {
		t.Fatal("invalid adaptation accepted", e)
	}
	if _, e := s.Store.FindEvent(context.Background(), "billing", "bad-fanout"); !errors.Is(e, domain.ErrNotFound) {
		t.Fatal("partial event committed", e)
	}
}
func TestClaimsConstraintsAndIndependentTargets(t *testing.T) {
	s := fixture(t, domain.Destination{ID: "a", Adapter: "crm.contact.v1", URL: "http://127.0.0.1:1", AllowHTTP: true, IdempotencyMode: "natural"}, domain.Destination{ID: "b", Adapter: "crm.contact.v1", URL: "http://127.0.0.1:1", AllowHTTP: true, IdempotencyMode: "natural"})
	r := submit(t, s, "one")
	if _, e := s.Store.DB.Exec("UPDATE notification_tasks SET state='running' WHERE id=?", r.TaskIds[0]); e == nil {
		t.Fatal("CHECK did not reject missing lease")
	}
	var wg sync.WaitGroup
	ids := make(chan uint64, 2)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, e := s.Store.Claim(context.Background(), "concurrent-worker", 45*time.Second)
			if e == nil {
				ids <- v.ID
			} else if !errors.Is(e, domain.ErrNotFound) {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[uint64]bool{}
	for id := range ids {
		if seen[id] {
			t.Fatal("task double claimed")
		}
		seen[id] = true
	} // SKIP LOCKED may temporarily see no rows while another transaction locks
	// the ready index. A subsequent polling round must collect remaining tasks.
	for len(seen) < 2 {
		v, e := s.Store.Claim(context.Background(), "poll-worker", 45*time.Second)
		if e != nil {
			t.Fatal("remaining target not claimable", e)
		}
		if seen[v.ID] {
			t.Fatal("task double claimed")
		}
		seen[v.ID] = true
	}
	a, e := s.Store.GetTask(context.Background(), r.TaskIds[0])
	if e != nil {
		t.Fatal(e)
	}
	b, e := s.Store.GetTask(context.Background(), r.TaskIds[1])
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Store.Finish(context.Background(), a, domain.Result{Outcome: "success", HTTPStatus: 200}); e != nil {
		t.Fatal(e)
	}
	if e = s.Store.Finish(context.Background(), b, domain.Result{Outcome: "retryable", HTTPStatus: 503}); e != nil {
		t.Fatal(e)
	}
	assertState(t, s, a.ID, domain.Succeeded)
	assertState(t, s, b.ID, domain.RetryWait)
}
func TestLeaseRecoveryAndLateResult(t *testing.T) {
	s := fixture(t)
	r := submit(t, s, "recover")
	old := claim(t, s)
	if _, e := s.Store.DB.Exec("UPDATE notification_tasks SET lease_until=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE id=?", old.ID); e != nil {
		t.Fatal(e)
	}
	if e := s.Store.Maintain(context.Background()); e != nil {
		t.Fatal(e)
	}
	assertState(t, s, old.ID, domain.RetryWait)
	ready(t, s, old.ID)
	current := claim(t, s)
	if current.LeaseToken == old.LeaseToken || current.AttemptCount != 2 {
		t.Fatal("lease not renewed")
	}
	if e := s.Store.Finish(context.Background(), old, domain.Result{Outcome: "success", HTTPStatus: 200}); !errors.Is(e, domain.ErrLeaseLost) {
		t.Fatal("stale result accepted", e)
	}
	assertState(t, s, current.ID, domain.Running)
	attempts, e := s.Store.Attempts(context.Background(), r.TaskIds[0], 0, 10)
	if e != nil || len(attempts) != 2 || attempts[0].Outcome != "unknown" || attempts[0].LateResultAt == nil {
		t.Fatal("recovery history missing", e)
	}
	if e = s.Store.Finish(context.Background(), current, domain.Result{Outcome: "success", HTTPStatus: 200}); e != nil {
		t.Fatal(e)
	}
	assertState(t, s, current.ID, domain.Succeeded)
}
func TestUnsafeRecoveryAndManagementIdempotency(t *testing.T) {
	s := fixture(t, domain.Destination{ID: "crm", Adapter: "crm.contact.v1", URL: "http://127.0.0.1:1", AllowHTTP: true, IdempotencyMode: "none"})
	r := submit(t, s, "unsafe")
	job := claim(t, s)
	if _, e := s.Store.DB.Exec("UPDATE notification_tasks SET lease_until=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE id=?", job.ID); e != nil {
		t.Fatal(e)
	}
	if e := s.Store.Maintain(context.Background()); e != nil {
		t.Fatal(e)
	}
	assertState(t, s, job.ID, domain.NeedsReview)
	replay := &pb.ReplayTaskRequest{TaskId: job.ID, OperationId: "replay-1", Reason: "verified externally"}
	if _, e := s.Replay(context.Background(), "test-operator", replay); !errors.Is(e, domain.ErrPrecondition) {
		t.Fatal("unsafe replay did not require evidence", e)
	}
	replay.VerificationEvidence = "provider confirmed zero processing"
	a, e := s.Replay(context.Background(), "test-operator", replay)
	if e != nil || a.RunNo != 2 || a.IdempotencyKey != job.IdempotencyKey || a.AttemptCount != 1 {
		t.Fatal("replay metadata wrong", e)
	}
	again, e := s.Replay(context.Background(), "test-operator", replay)
	if e != nil || again.RunNo != 2 {
		t.Fatal("duplicate replay started another run", e)
	}
	conflict := proto.Clone(replay).(*pb.ReplayTaskRequest)
	conflict.Reason = "different"
	if _, e = s.Replay(context.Background(), "test-operator", conflict); !errors.Is(e, domain.ErrConflict) {
		t.Fatal("operation conflict ignored", e)
	}
	next := claim(t, s)
	if e = s.Store.Finish(context.Background(), next, domain.Result{Outcome: "permanent_failure", HTTPStatus: 400}); e != nil {
		t.Fatal(e)
	}
	resolved, e := s.Resolve(context.Background(), "test-operator", &pb.ResolveTaskRequest{TaskId: next.ID, OperationId: "close-1", Reason: "permanent rejection", VerificationEvidence: "provider confirmed rejection"})
	if e != nil || resolved.State != domain.Failed || resolved.ClosedAt == nil {
		t.Fatal("failure not closed", e)
	}
	var ops int
	if e = s.Store.DB.QueryRow("SELECT COUNT(*) FROM notification_operations WHERE task_id=?", r.TaskIds[0]).Scan(&ops); e != nil || ops != 2 {
		t.Fatal("audit count wrong", e, ops)
	}
}
func TestRetryBudgetDeadlineAndWorkerLock(t *testing.T) {
	s := fixture(t, domain.Destination{ID: "crm", Adapter: "crm.contact.v1", URL: "http://127.0.0.1:1", AllowHTTP: true, IdempotencyMode: "natural", MaxAttempts: 1})
	r := submit(t, s, "budget")
	job := claim(t, s)
	if e := s.Store.Finish(context.Background(), job, domain.Result{Outcome: "retryable", HTTPStatus: 503}); e != nil {
		t.Fatal(e)
	}
	assertState(t, s, r.TaskIds[0], domain.Failed)
	expired := submit(t, s, "deadline")
	if _, e := s.Store.DB.Exec("UPDATE notification_tasks SET deadline_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE id=?", expired.TaskIds[0]); e != nil {
		t.Fatal(e)
	}
	if e := s.Store.Maintain(context.Background()); e != nil {
		t.Fatal(e)
	}
	assertState(t, s, expired.TaskIds[0], domain.Failed)
	lock, e := s.Store.AcquireWorker(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if !lock.Alive(context.Background()) {
		t.Fatal("worker lock not alive")
	}
	if second, e := s.Store.AcquireWorker(context.Background()); !errors.Is(e, domain.ErrPrecondition) {
		if second != nil {
			second.Close()
		}
		t.Fatal("second worker acquired lock", e)
	}
	lock.Close()
	next, e := s.Store.AcquireWorker(context.Background())
	if e != nil {
		t.Fatal("worker lock not released", e)
	}
	next.Close()
}
func rpcFixture(t *testing.T, s *app.Service) (pb.NotificationServiceClient, pb.ManagementServiceClient) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	secrets := func(k string) (string, bool) {
		v, ok := map[string]string{"SOURCE_A": "billing-token", "SOURCE_B": "other-token", "ADMIN": "admin-token"}[k]
		return v, ok
	}
	server, _ := serviceRPC.New(s, secrets, true)
	go server.Serve(listener)
	conn, e := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { conn.Close(); server.Stop(); listener.Close() })
	return pb.NewNotificationServiceClient(conn), pb.NewManagementServiceClient(conn)
}
func auth(token string) context.Context {
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
}
func TestGRPCAuthenticationQueryAndManagement(t *testing.T) {
	s := fixture(t)
	client, manager := rpcFixture(t, s)
	if _, e := client.SubmitEvent(context.Background(), &pb.SubmitEventRequest{Event: event("rpc")}); status.Code(e) != codes.Unauthenticated {
		t.Fatal(e)
	}
	r, e := client.SubmitEvent(auth("billing-token"), &pb.SubmitEventRequest{Event: event("rpc")})
	if e != nil || !r.Accepted {
		t.Fatal(e)
	}
	if _, e = client.GetEvent(auth("other-token"), &pb.GetEventRequest{Id: r.Id}); status.Code(e) != codes.NotFound {
		t.Fatal("cross-source event exposed", e)
	}
	if _, e = client.GetTask(auth("other-token"), &pb.GetTaskRequest{Id: r.TaskIds[0]}); status.Code(e) != codes.NotFound {
		t.Fatal("cross-source task exposed", e)
	}
	spoof := event("spoof")
	spoof.Source = "other"
	if _, e = client.SubmitEvent(auth("billing-token"), &pb.SubmitEventRequest{Event: spoof}); status.Code(e) != codes.PermissionDenied {
		t.Fatal("spoofed source accepted", e)
	}
	if _, e = manager.ListTasks(auth("billing-token"), &pb.ListTasksRequest{}); status.Code(e) != codes.PermissionDenied {
		t.Fatal("business identity allowed management", e)
	}
	page, e := manager.ListTasks(auth("admin-token"), &pb.ListTasksRequest{Limit: 1})
	if e != nil || len(page.Tasks) != 1 {
		t.Fatal("admin listing failed", e)
	}
	job := claim(t, s)
	if e = s.Store.Finish(context.Background(), job, domain.Result{Outcome: "permanent_failure", HTTPStatus: 400}); e != nil {
		t.Fatal(e)
	}
	replay, e := manager.ReplayTask(auth("admin-token"), &pb.ReplayTaskRequest{TaskId: job.ID, OperationId: "rpc-replay", Reason: "fixed provider"})
	if e != nil || replay.RunNo != 2 {
		t.Fatal("RPC replay failed", e)
	}
	attempts, e := manager.ListAttempts(auth("admin-token"), &pb.ListAttemptsRequest{TaskId: job.ID})
	if e != nil || len(attempts.Attempts) != 1 {
		t.Fatal("RPC history failed", e)
	}
}
func TestWorkerEndToEndAndRetry(t *testing.T) {
	var requests atomic.Int32
	var active atomic.Int32
	var peak atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		now := active.Add(1)
		defer active.Add(-1)
		for {
			old := peak.Load()
			if now <= old || peak.CompareAndSwap(old, now) {
				break
			}
		}
		time.Sleep(15 * time.Millisecond)
		if requests.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"success":true}`)
	}))
	defer srv.Close()
	s := fixture(t, domain.Destination{ID: "crm", Adapter: "crm.contact.v1", URL: srv.URL, AllowHTTP: true, IdempotencyMode: "natural", Retry: domain.RetryPolicy{BaseMS: 1, CapMS: 1}, Success: domain.SuccessRule{Statuses: []int{200}, JSONField: "success", JSONValue: "true"}})
	metrics := observability.New()
	sender := transport.New(nil)
	defer sender.Client.CloseIdleConnections()
	runner := &worker.Runner{Store: s.Store, Config: s.Config, Sender: sender, Metrics: metrics, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), PollInterval: 10 * time.Millisecond, MaxWorkers: 2}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case e := <-done:
			if e != nil {
				t.Error(e)
			}
		case <-time.After(25 * time.Second):
			t.Error("worker did not stop")
		}
	})
	client, _ := rpcFixture(t, s)
	var ids []uint64
	for i := 0; i < 4; i++ {
		e := event(domain.UUID())
		requestCtx, stop := context.WithTimeout(auth("billing-token"), 3*time.Second)
		r, err := client.SubmitEvent(requestCtx, &pb.SubmitEventRequest{Event: e})
		stop()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.TaskIds[0])
	}
	until := time.Now().Add(8 * time.Second)
	for {
		all := true
		for _, id := range ids {
			v, e := s.Store.GetTask(context.Background(), id)
			if e != nil {
				t.Fatal(e)
			}
			if v.State != domain.Succeeded {
				all = false
			}
		}
		if all {
			break
		}
		if time.Now().After(until) {
			t.Fatal("tasks did not complete")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if requests.Load() != 5 || peak.Load() > 2 {
		t.Fatalf("requests=%d peak=%d", requests.Load(), peak.Load())
	}
	var tries int
	if e := s.Store.DB.QueryRow("SELECT COUNT(*) FROM notification_attempts").Scan(&tries); e != nil || tries != 5 {
		t.Fatal("attempt history missing", e, tries)
	}
	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	if recorder.Code != 200 || !strings.Contains(recorder.Body.String(), "notification_delivery_attempts_total") {
		t.Fatal("metrics missing")
	}
}
func TestSnapshotAndProviderIdempotency(t *testing.T) {
	s := fixture(t, domain.Destination{ID: "crm", Adapter: "crm.contact.v1", URL: "http://127.0.0.1:1", AllowHTTP: true, IdempotencyMode: "provider_key", IdempotencyHeader: "Idempotency-Key", IdempotencyRetentionSeconds: 60})
	r := submit(t, s, "snapshot")
	s.Config.Destinations[0].URL = "http://127.0.0.1:2"
	job := claim(t, s)
	if job.Request.URL != "http://127.0.0.1:1" || job.ValidUntil == nil || job.FirstAttemptAt == nil {
		t.Fatal("snapshot or window missing")
	}
	if e := s.Store.Finish(context.Background(), job, domain.Result{Outcome: "unknown", ErrorCode: "REQUEST_TIMEOUT"}); e != nil {
		t.Fatal(e)
	}
	ready(t, s, job.ID)
	next := claim(t, s)
	if next.IdempotencyKey != job.IdempotencyKey || !next.ValidUntil.Equal(*job.ValidUntil) {
		t.Fatal("retry changed provider key/window")
	}
	if _, e := s.Store.DB.Exec("UPDATE notification_tasks SET idempotency_valid_until=DATE_ADD(first_attempt_at,INTERVAL 1 MICROSECOND),lease_until=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE id=?", next.ID); e != nil {
		t.Fatal(e)
	}
	if e := s.Store.Maintain(context.Background()); e != nil {
		t.Fatal(e)
	}
	assertState(t, s, r.TaskIds[0], domain.NeedsReview)
	if _, e := s.Replay(context.Background(), "test-operator", &pb.ReplayTaskRequest{TaskId: job.ID, OperationId: "expired-key", Reason: "needs fresh key", VerificationEvidence: "externally verified"}); !errors.Is(e, domain.ErrPrecondition) {
		t.Fatal("expired window reused", e)
	}
	data, _ := json.Marshal(job)
	if strings.Contains(string(data), "secret_ref") || strings.Contains(string(data), "request_body") {
		t.Fatal("management summary contains prepared request")
	}
}

func TestExpiredLeaseResultRequiresRecovery(t *testing.T) {
	s := fixture(t)
	submit(t, s, "expired-completion")
	job := claim(t, s)
	if _, e := s.Store.DB.Exec("UPDATE notification_tasks SET lease_until=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE id=?", job.ID); e != nil {
		t.Fatal(e)
	}
	if e := s.Store.Finish(context.Background(), job, domain.Result{Outcome: "success", HTTPStatus: 200}); !errors.Is(e, domain.ErrLeaseLost) {
		t.Fatal("expired lease accepted result", e)
	}
	assertState(t, s, job.ID, domain.RetryWait)
	attempts, e := s.Store.Attempts(context.Background(), job.ID, 0, 10)
	if e != nil || len(attempts) != 1 || attempts[0].Outcome != "unknown" || attempts[0].LateResultAt == nil {
		t.Fatal("late completion not audited", e)
	}
}

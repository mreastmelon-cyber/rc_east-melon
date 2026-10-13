// Command evidence runs a fresh purchase through the local Compose service and
// saves actual RPC responses, database records and container logs for inspection.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	pb "github.com/mreastmelon-cyber/right-capital-project/api/notification/v1"
	"github.com/mreastmelon-cyber/right-capital-project/internal/domain"
	"github.com/mreastmelon-cyber/right-capital-project/internal/repository"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	out := flag.String("out", "", "new output directory (must not already exist)")
	flag.Parse()
	if *out == "" {
		fmt.Fprintln(os.Stderr, "-out is required")
		os.Exit(1)
	}
	if err := run(*out); err != nil {
		fmt.Fprintln(os.Stderr, "evidence collection failed:", err)
		os.Exit(1)
	}
}
func save(out, name string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile(filepath.Join(out, name), append(b, '\n'), 0644)
}
func saveProto(out, name string, v proto.Message) error {
	b, e := (protojson.MarshalOptions{Indent: "  ", EmitUnpopulated: true}).Marshal(v)
	if e != nil {
		return e
	}
	return os.WriteFile(filepath.Join(out, name), append(b, '\n'), 0644)
}
func compose(ctx context.Context, args ...string) ([]byte, error) {
	base := []string{"compose", "--env-file", ".env.example", "-f", "deploy/compose.yaml"}
	return exec.CommandContext(ctx, "docker", append(base, args...)...).Output()
}
func run(out string) error {
	if os.Getenv("SHOP_RPC_TOKEN") == "" || os.Getenv("ADMIN_RPC_TOKEN") == "" || os.Getenv("MYSQL_DSN") == "" {
		return errors.New("load local environment variables first")
	}
	if err := os.Mkdir(out, 0755); err != nil {
		return err
	}
	started := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	conn, e := grpc.NewClient("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		return e
	}
	defer conn.Close()
	h, e := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if e != nil {
		return e
	}
	if h.Status != healthpb.HealthCheckResponse_SERVING {
		return errors.New("service is not ready")
	}
	status, e := compose(ctx, "ps", "--format", "json")
	if e != nil {
		return e
	}
	if e = os.WriteFile(filepath.Join(out, "01-compose-status.txt"), status, 0644); e != nil {
		return e
	}
	if e = save(out, "01-health.json", map[string]any{"checked_at": time.Now().UTC(), "grpc_address": "127.0.0.1:50051", "status": h.Status.String()}); e != nil {
		return e
	}
	client := pb.NewNotificationServiceClient(conn)
	manager := pb.NewManagementServiceClient(conn)
	source := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+os.Getenv("SHOP_RPC_TOKEN"))
	admin := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+os.Getenv("ADMIN_RPC_TOKEN"))
	eventID := "evidence-purchase-" + started.Format("20060102T150405") + "-" + domain.UUID()[:8]
	request := &pb.SubmitEventRequest{Event: &pb.Event{EventId: eventID, Source: "shop", EventType: "purchase.completed", SchemaVersion: 1, OccurredAt: timestamppb.New(started), Data: &pb.Event_Purchase{Purchase: &pb.Purchase{OrderId: "evidence-order-" + domain.UUID()[:8], ProductId: "evidence-product-001", Quantity: 2}}}}
	if e = saveProto(out, "02-submit-request.json", request); e != nil {
		return e
	}
	sendAt := time.Now().UTC()
	accepted, e := client.SubmitEvent(source, request)
	if e != nil {
		return e
	}
	receivedAt := time.Now().UTC()
	if !accepted.Accepted || accepted.Duplicate || len(accepted.TaskIds) != 1 {
		return errors.New("fresh event was not accepted exactly once")
	}
	if e = saveProto(out, "02-submit-response.json", accepted); e != nil {
		return e
	}
	if e = save(out, "02-rpc-timing.json", map[string]any{"method": pb.NotificationService_SubmitEvent_FullMethodName, "sent_at": sendAt, "returned_at": receivedAt, "elapsed_ms": receivedAt.Sub(sendAt).Milliseconds(), "grpc_status": "OK", "identity": "shop (credential omitted)"}); e != nil {
		return e
	}
	id := accepted.TaskIds[0]
	var task *pb.TaskView
	var states []any
	for {
		task, e = client.GetTask(source, &pb.GetTaskRequest{Id: id})
		if e != nil {
			return e
		}
		if len(states) == 0 || states[len(states)-1].(map[string]any)["state"] != task.State {
			states = append(states, map[string]any{"observed_at": time.Now().UTC(), "state": task.State, "attempt_count": task.AttemptCount})
		}
		if task.State == domain.Succeeded {
			break
		}
		if task.State == domain.Failed || task.State == domain.NeedsReview {
			return fmt.Errorf("task ended as %s", task.State)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	if e = save(out, "04-observed-states.json", states); e != nil {
		return e
	}
	if e = saveProto(out, "04-task.json", task); e != nil {
		return e
	}
	event, e := client.GetEvent(source, &pb.GetEventRequest{Id: accepted.Id})
	if e != nil {
		return e
	}
	if e = saveProto(out, "04-event.json", event); e != nil {
		return e
	}
	attempts, e := manager.ListAttempts(admin, &pb.ListAttemptsRequest{TaskId: id})
	if e != nil {
		return e
	}
	if len(attempts.Attempts) != 1 || attempts.Attempts[0].Outcome != "success" || attempts.Attempts[0].HttpStatus != 200 {
		return errors.New("expected one successful HTTP 200 attempt")
	}
	if e = saveProto(out, "04-attempts.json", attempts); e != nil {
		return e
	}
	duplicate, e := client.SubmitEvent(source, request)
	if e != nil {
		return e
	}
	if !duplicate.Duplicate || duplicate.Id != accepted.Id || len(duplicate.TaskIds) != 1 || duplicate.TaskIds[0] != id {
		return errors.New("duplicate event created a new task")
	}
	if e = saveProto(out, "05-duplicate-response.json", duplicate); e != nil {
		return e
	}
	store, e := repository.Open(ctx, os.Getenv("MYSQL_DSN"))
	if e != nil {
		return errors.New("evidence database connection failed")
	}
	defer store.DB.Close()
	persisted, e := store.GetTask(ctx, id)
	if e != nil {
		return e
	}
	var eventCount, taskCount, attemptCount int
	if e = store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM notification_events WHERE source_system='shop' AND source_event_id=?", eventID).Scan(&eventCount); e != nil {
		return e
	}
	if e = store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM notification_tasks WHERE event_id=?", accepted.Id).Scan(&taskCount); e != nil {
		return e
	}
	if e = store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM notification_attempts WHERE task_id=?", id).Scan(&attemptCount); e != nil {
		return e
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(persisted.Request.Body))
	row := map[string]any{"checked_at": time.Now().UTC(), "event_id": accepted.Id, "source_event_id": eventID, "task_id": id, "destination_id": persisted.DestinationID, "state": persisted.State, "attempt_count": persisted.AttemptCount, "idempotency_key": persisted.IdempotencyKey, "request_method": persisted.Request.Method, "request_url": persisted.Request.URL, "request_body": json.RawMessage(persisted.Request.Body), "body_sha256": hash, "last_http_status": persisted.LastHTTPStatus, "completed_at": persisted.CompletedAt, "event_rows": eventCount, "task_rows": taskCount, "attempt_rows": attemptCount}
	if persisted.State != domain.Succeeded || persisted.LastHTTPStatus != 200 || eventCount != 1 || taskCount != 1 || attemptCount != 1 {
		return errors.New("database verification failed")
	}
	if e = save(out, "04-database.json", row); e != nil {
		return e
	}
	logs, e := compose(ctx, "logs", "--no-color", "--timestamps", "--since", started.Format(time.RFC3339), "notification", "mock-provider")
	if e != nil {
		return e
	}
	var provider, worker []string
	scanner := bufio.NewScanner(bytes.NewReader(logs))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "mock notification accepted") && strings.Contains(line, persisted.IdempotencyKey) && strings.Contains(line, hash) {
			provider = append(provider, line)
		}
		if strings.Contains(line, "delivery attempt completed") && strings.Contains(line, fmt.Sprintf(`"task_id":%d,`, id)) {
			worker = append(worker, line)
		}
	}
	if e = scanner.Err(); e != nil {
		return e
	}
	if len(provider) != 1 || len(worker) != 1 {
		return errors.New("matching provider receipt or Worker completion log missing")
	}
	if e = os.WriteFile(filepath.Join(out, "03-provider.log"), []byte(strings.Join(provider, "\n")+"\n"), 0644); e != nil {
		return e
	}
	if e = os.WriteFile(filepath.Join(out, "03-worker.log"), []byte(strings.Join(worker, "\n")+"\n"), 0644); e != nil {
		return e
	}
	manifest := map[string]any{"run_started_at": started, "run_completed_at": time.Now().UTC(), "source_event_id": eventID, "event_id": accepted.Id, "task_id": id, "provider": "local mock inventory API", "idempotency_key": persisted.IdempotencyKey, "body_sha256": hash, "result": "PASS", "checks": []string{"gRPC health SERVING", "fresh event accepted without duplicate", "mock provider receipt matches idempotency key and body hash", "Worker success log matches task ID", "task succeeded; HTTP 200; exactly one attempt", "MySQL persisted 1 event / 1 task / 1 attempt", "repeat submission returns same event and task"}}
	if e = save(out, "manifest.json", manifest); e != nil {
		return e
	}
	fmt.Printf("PASS: event=%s event_id=%d task_id=%d; provider confirmed receipt, HTTP 200, succeeded, duplicate deduplicated\nEvidence: %s\n", eventID, accepted.Id, id, out)
	return nil
}

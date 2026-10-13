// Command smoke verifies the locally running Compose stack against all examples.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	pb "github.com/mreastmelon-cyber/right-capital-project/api/notification/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, "smoke test failed:", e)
		os.Exit(1)
	}
}
func run() error {
	// This deliberately tests the local development stack, using no production credentials.
	conn, e := grpc.NewClient("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		return e
	}
	defer conn.Close()
	client := pb.NewNotificationServiceClient(conn)
	manager := pb.NewManagementServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, sample := range []struct{ file, ref string }{{"registration", "IDENTITY_RPC_TOKEN"}, {"payment", "BILLING_RPC_TOKEN"}, {"purchase", "SHOP_RPC_TOKEN"}} {
		token := os.Getenv(sample.ref)
		admin := os.Getenv("ADMIN_RPC_TOKEN")
		if token == "" || admin == "" {
			return errors.New("load the local .env tokens first")
		}
		source := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		raw, e := os.ReadFile("examples/" + sample.file + ".json")
		if e != nil {
			return e
		}
		req := &pb.SubmitEventRequest{}
		if e = protojson.Unmarshal(raw, req); e != nil {
			return e
		}
		accepted, e := client.SubmitEvent(source, req)
		if e != nil {
			return e
		}
		if !accepted.Accepted || len(accepted.TaskIds) != 1 {
			return errors.New("event not accepted with one target")
		}
		duplicate, e := client.SubmitEvent(source, req)
		if e != nil {
			return e
		}
		if !duplicate.Duplicate || duplicate.Id != accepted.Id {
			return errors.New("duplicate event created a new event")
		}
		id := accepted.TaskIds[0]
		for {
			task, e := client.GetTask(source, &pb.GetTaskRequest{Id: id})
			if e != nil {
				return e
			}
			if task.State == "succeeded" {
				break
			}
			if task.State == "failed" || task.State == "needs_review" {
				return fmt.Errorf("%s entered %s", sample.file, task.State)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}
		attempts, e := manager.ListAttempts(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+admin), &pb.ListAttemptsRequest{TaskId: id})
		if e != nil {
			return e
		}
		if len(attempts.Attempts) == 0 || attempts.Attempts[len(attempts.Attempts)-1].Outcome != "success" {
			return errors.New("successful attempt missing")
		}
		fmt.Printf("%s: accepted, deduplicated, delivered, history verified (task %d)\n", sample.file, id)
	}
	return nil
}

package domain

import (
	"testing"
	"time"
)

func TestCompletionSafety(t *testing.T) {
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name, mode, outcome string
		notProcessed        bool
		expired             bool
		attempts            uint32
		want                string
	}{
		{"success", "none", "success", false, false, 1, Succeeded},
		{"unsafe timeout", "none", "unknown", false, false, 1, NeedsReview},
		{"confirmed not sent", "none", "retryable", true, false, 1, RetryWait},
		{"provider idempotency", "provider_key", "unknown", false, false, 1, RetryWait},
		{"expired provider key", "provider_key", "unknown", false, true, 1, NeedsReview},
		{"naturally idempotent", "natural", "retryable", false, false, 1, RetryWait},
		{"attempts exhausted", "natural", "retryable", false, false, 12, Failed},
		{"explicit rejection", "none", "permanent_failure", false, false, 1, Failed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			until := now.Add(time.Hour)
			if c.expired {
				until = now.Add(-time.Second)
			}
			job := Task{State: Running, AttemptCount: c.attempts, RunAttemptCount: c.attempts, MaxAttempts: 12, DeadlineAt: now.Add(2 * time.Hour), Policy: RetryPolicy{BaseMS: 1, CapMS: 1}, IdempotencyMode: c.mode, ValidUntil: &until, LeaseToken: "old", LeaseUntil: &until}
			got := Complete(job, Result{Outcome: c.outcome, NotProcessed: c.notProcessed}, now)
			if got.State != c.want {
				t.Fatalf("got %s want %s", got.State, c.want)
			}
			if got.LeaseToken != "" || got.LeaseUntil != nil {
				t.Fatal("lease retained after completion")
			}
			if (got.NextAttemptAt != nil) != (c.want == RetryWait) {
				t.Fatal("wrong scheduling state")
			}
			if (got.CompletedAt != nil) != (c.want != RetryWait) {
				t.Fatal("wrong completion time")
			}
		})
	}
}
func TestRetryWindowAndDeadline(t *testing.T) {
	now := time.Now().UTC()
	job := Task{AttemptCount: 1, RunAttemptCount: 1, MaxAttempts: 12, DeadlineAt: now.Add(time.Hour), Policy: RetryPolicy{BaseMS: 1, CapMS: 1}, IdempotencyMode: "provider_key", ValidUntil: Ptr(now.Add(500 * time.Millisecond))}
	if got := Complete(job, Result{Outcome: "unknown"}, now); got.State != NeedsReview {
		t.Fatal(got.State)
	}
	job.IdempotencyMode = "natural"
	job.DeadlineAt = now.Add(500 * time.Millisecond)
	if got := Complete(job, Result{Outcome: "retryable"}, now); got.State != Failed {
		t.Fatal("deadline ignored")
	}
}
func TestIdentifiers(t *testing.T) {
	for _, s := range []string{"", " whitespace", "中文", "line\n"} {
		if Identifier(s, 128) {
			t.Errorf("accepted %q", s)
		}
	}
	if !Identifier("source.event-123", 128) {
		t.Fatal("valid identifier rejected")
	}
	if len(UUID()) != 36 {
		t.Fatal("invalid UUID")
	}
}

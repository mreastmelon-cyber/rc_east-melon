package transport

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mreastmelon-cyber/right-capital-project/internal/domain"
)

func taskFor(url string) domain.Task {
	return domain.Task{Request: domain.Prepared{Method: "POST", URL: url, ContentType: "application/json", Body: []byte(`{"id":"1"}`)}, Config: domain.Destination{TimeoutMS: 1000, Auth: domain.Auth{Type: "none"}, Success: domain.SuccessRule{Statuses: []int{200, 204}}}, DeadlineAt: time.Now().Add(time.Hour), IdempotencyMode: "natural"}
}
func TestResponseContracts(t *testing.T) {
	cases := []struct {
		name              string
		status            int
		body, field, want string
	}{
		{"json accepted", 200, `{"success":true}`, "success", "success"},
		{"business rejection", 200, `{"success":false}`, "success", "permanent_failure"},
		{"missing contract", 200, `{}`, "success", "unknown"},
		{"invalid json", 200, `bad`, "success", "unknown"},
		{"trailing data", 200, `{"success":true} {}`, "success", "unknown"},
		{"no content", 204, "", "", "success"},
		{"server fault", 503, `busy`, "", "retryable"},
		{"invalid request", 400, `bad`, "", "permanent_failure"},
		{"oversized body", 200, strings.Repeat("x", (256<<10)+1), "", "unknown"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(c.status); io.WriteString(w, c.body) }))
			defer srv.Close()
			job := taskFor(srv.URL)
			job.Config.Success.JSONField = c.field
			job.Config.Success.JSONValue = "true"
			s := New(nil)
			defer s.Client.CloseIdleConnections()
			result := s.Send(context.Background(), job)
			if result.Outcome != c.want {
				t.Fatalf("got %s want %s (%s)", result.Outcome, c.want, result.ErrorCode)
			}
			if len(c.body) > 256<<10 && !result.Truncated {
				t.Fatal("large body not flagged")
			}
		})
	}
}
func TestNoRedirectOrCredentialLeak(t *testing.T) {
	var called atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called.Store(true) }))
	defer target.Close()
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer src.Close()
	sender := New(nil)
	defer sender.Client.CloseIdleConnections()
	r := sender.Send(context.Background(), taskFor(src.URL))
	if r.ErrorCode != "REDIRECT_REJECTED" || called.Load() {
		t.Fatal("redirect followed")
	}
}
func TestTimeoutAndAuthentication(t *testing.T) {
	timeout := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); <-r.Context().Done() }))
	job := taskFor(timeout.URL)
	job.Config.TimeoutMS = 20
	sender := New(nil)
	result := sender.Send(context.Background(), job)
	sender.Client.CloseIdleConnections()
	timeout.Close()
	if result.Outcome != "unknown" || result.NotProcessed {
		t.Fatalf("unsafe timeout classification: %+v", result)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		m := hmac.New(sha256.New, []byte("secret"))
		m.Write([]byte(r.Header.Get("X-Timestamp") + "\n"))
		m.Write(body)
		if r.Header.Get("Idempotency-Key") != "stable-key" || r.Header.Get("X-Signature") != hex.EncodeToString(m.Sum(nil)) {
			t.Error("signature or key missing")
		}
		w.WriteHeader(204)
	}))
	defer srv.Close()
	job = taskFor(srv.URL)
	job.IdempotencyMode = "provider_key"
	job.IdempotencyKey = "stable-key"
	job.ValidUntil = domain.Ptr(time.Now().Add(time.Hour))
	job.Config.IdempotencyHeader = "Idempotency-Key"
	job.Config.Auth = domain.Auth{Type: "hmac", SecretRef: "KEY"}
	sender = New(func(ref string) (string, bool) { return "secret", ref == "KEY" })
	defer sender.Client.CloseIdleConnections()
	if result = sender.Send(context.Background(), job); result.Outcome != "success" {
		t.Fatal(result)
	}
	job.ValidUntil = domain.Ptr(time.Now().Add(-time.Second))
	if result = sender.Send(context.Background(), job); result.ErrorCode != "IDEMPOTENCY_WINDOW_EXPIRED" {
		t.Fatal("expired key sent")
	}
}
func TestCredentialsUnavailable(t *testing.T) {
	job := taskFor("https://example.com")
	job.Config.Auth = domain.Auth{Type: "bearer", SecretRef: "missing"}
	r := New(func(string) (string, bool) { return "", false }).Send(context.Background(), job)
	if r.ErrorCode != "CREDENTIAL_UNAVAILABLE" || r.Outcome != "permanent_failure" {
		t.Fatal(r)
	}
}

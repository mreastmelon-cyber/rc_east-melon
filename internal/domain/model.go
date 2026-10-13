package domain

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	randv2 "math/rand/v2"
	"time"
)

var (
	ErrInvalid      = errors.New("invalid request")
	ErrConflict     = errors.New("identifier conflicts with existing content")
	ErrNotFound     = errors.New("record not found")
	ErrPrecondition = errors.New("operation precondition not satisfied")
	ErrLeaseLost    = errors.New("execution lease lost")
	ErrUnavailable  = errors.New("storage unavailable")
)

const (
	Pending     = "pending"
	Running     = "running"
	RetryWait   = "retry_wait"
	Succeeded   = "succeeded"
	Failed      = "failed"
	NeedsReview = "needs_review"
)

func ValidState(s string) bool {
	switch s {
	case Pending, Running, RetryWait, Succeeded, Failed, NeedsReview:
		return true
	}
	return false
}
func Identifier(s string, max int) bool {
	if len(s) == 0 || len(s) > max {
		return false
	}
	for _, c := range []byte(s) {
		if c <= 32 || c >= 127 {
			return false
		}
	}
	return true
}
func UUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

type Auth struct {
	Type      string `json:"type"`
	SecretRef string `json:"secret_ref"`
	Header    string `json:"header,omitempty"`
	Username  string `json:"username,omitempty"`
}
type SuccessRule struct {
	Statuses  []int  `json:"statuses"`
	JSONField string `json:"json_field,omitempty"`
	JSONValue string `json:"json_value,omitempty"`
}
type RetryPolicy struct {
	BaseMS int64 `json:"base_ms"`
	CapMS  int64 `json:"cap_ms"`
}
type Destination struct {
	ID                          string            `json:"id"`
	Adapter                     string            `json:"adapter"`
	URL                         string            `json:"url"`
	AllowHTTP                   bool              `json:"allow_http,omitempty"`
	Headers                     map[string]string `json:"headers,omitempty"`
	Auth                        Auth              `json:"auth"`
	Success                     SuccessRule       `json:"success"`
	TimeoutMS                   int64             `json:"timeout_ms"`
	MaxAttempts                 uint32            `json:"max_attempts"`
	DeadlineSeconds             int64             `json:"deadline_seconds"`
	Retry                       RetryPolicy       `json:"retry"`
	IdempotencyMode             string            `json:"idempotency_mode"`
	IdempotencyHeader           string            `json:"idempotency_header,omitempty"`
	IdempotencyRetentionSeconds uint32            `json:"idempotency_retention_seconds,omitempty"`
}
type Prepared struct {
	Method, URL, ContentType string
	Headers                  map[string]string
	Body                     []byte
}
type Event struct {
	ID                          uint64
	Source, SourceEventID, Type string
	SchemaVersion               uint32
	OccurredAt                  time.Time
	Payload                     json.RawMessage
	Hash                        [32]byte
	HashVersion                 uint16
	ConfigVersion               string
	ReceivedAt                  time.Time
}
type Task struct {
	ID, EventID                                       uint64
	DestinationID, AdapterKey, ConfigVersion          string
	Config                                            Destination `json:"-"`
	Request                                           Prepared    `json:"-"`
	Policy                                            RetryPolicy `json:"-"`
	IdempotencyMode, IdempotencyKey                   string
	RetentionSeconds                                  *uint32
	FirstAttemptAt, ValidUntil                        *time.Time
	State                                             string
	AttemptCount, RunNo, RunAttemptCount, MaxAttempts uint32
	DeadlineAt                                        time.Time
	NextAttemptAt                                     *time.Time
	LeaseToken                                        string     `json:"-"`
	LeaseUntil                                        *time.Time `json:"-"`
	LastHTTPStatus                                    int
	LastProviderCode, LastErrorCode, LastErrorSummary string
	CompletedAt, ClosedAt                             *time.Time
	ClosedBy                                          string
	CreatedAt, UpdatedAt                              time.Time
}
type Attempt struct {
	ID, TaskID                            uint64
	AttemptNo, RunNo                      uint32
	LeaseToken, WorkerID, Outcome         string
	StartedAt                             time.Time
	FinishedAt                            *time.Time
	DurationMS                            *uint32
	HTTPStatus                            int
	ProviderCode, ErrorCode, ErrorSummary string
	ResponseSummary                       json.RawMessage
	Truncated                             bool
	LateResultAt                          *time.Time
	LateResult                            json.RawMessage
}
type Result struct {
	Outcome                               string
	HTTPStatus                            int
	ProviderCode, ErrorCode, ErrorSummary string
	Summary                               json.RawMessage
	Truncated                             bool
	DurationMS                            uint32
	NotProcessed                          bool
}
type Operation struct {
	Operator, ID, Type, Reason string
	Hash                       [32]byte
	Request                    json.RawMessage
}
type ListFilter struct {
	Destination, State string
	AfterID            uint64
	Limit              int
}

func Ptr[T any](v T) *T { return &v }
func (t Task) Safe(now time.Time) bool {
	if t.AttemptCount == 0 {
		return true
	}
	switch t.IdempotencyMode {
	case "natural":
		return true
	case "provider_key":
		return t.ValidUntil != nil && now.Before(*t.ValidUntil)
	}
	return false
}
func (t Task) RetryTime(now time.Time) time.Time {
	n := int(t.RunAttemptCount) - 1
	if n < 0 {
		n = 0
	}
	if n > 30 {
		n = 30
	}
	capms := math.Min(float64(t.Policy.CapMS), float64(t.Policy.BaseMS)*math.Pow(2, float64(n)))
	delay := time.Duration(randv2.Int64N(max(int64(capms), 1))) * time.Millisecond
	if delay < time.Second {
		delay = time.Second
	}
	next := now.Add(delay)
	return next
}
func Complete(t Task, r Result, now time.Time) Task {
	t.UpdatedAt = now
	t.LeaseToken = ""
	t.LeaseUntil = nil
	t.NextAttemptAt = nil
	t.LastHTTPStatus = r.HTTPStatus
	t.LastProviderCode = r.ProviderCode
	t.LastErrorCode = r.ErrorCode
	t.LastErrorSummary = r.ErrorSummary
	switch {
	case r.Outcome == "success":
		t.State = Succeeded
	case r.Outcome == "permanent_failure":
		t.State = Failed
	case !r.NotProcessed && !t.Safe(now):
		t.State = NeedsReview
	default:
		next := t.RetryTime(now)
		switch {
		case t.RunAttemptCount >= t.MaxAttempts || !next.Before(t.DeadlineAt):
			t.State = Failed
			t.LastErrorCode = "RETRY_BUDGET_EXHAUSTED"
		case !r.NotProcessed && t.IdempotencyMode == "provider_key" && (t.ValidUntil == nil || !next.Before(*t.ValidUntil)):
			t.State = NeedsReview
			t.LastErrorCode = "IDEMPOTENCY_WINDOW_EXPIRED"
		default:
			t.State = RetryWait
			t.NextAttemptAt = &next
		}
	}
	if t.State != RetryWait {
		t.CompletedAt = &now
	} else {
		t.CompletedAt = nil
	}
	return t
}

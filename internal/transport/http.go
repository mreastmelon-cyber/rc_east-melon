package transport

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/mreastmelon-cyber/right-capital-project/internal/adapter"
	"github.com/mreastmelon-cyber/right-capital-project/internal/domain"
)

type SecretResolver func(string) (string, bool)
type Sender struct {
	Client  *http.Client
	Secrets SecretResolver
}

func New(secrets SecretResolver) *Sender {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DialContext = (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	tr.TLSHandshakeTimeout = 3 * time.Second
	tr.MaxIdleConns = 64
	tr.MaxIdleConnsPerHost = 32
	return &Sender{Client: &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, Secrets: secrets}
}
func (s *Sender) Send(ctx context.Context, t domain.Task) domain.Result {
	start := time.Now()
	result := s.send(ctx, t)
	result.DurationMS = uint32(time.Since(start).Milliseconds())
	return result
}
func (s *Sender) send(ctx context.Context, t domain.Task) domain.Result {
	now := time.Now().UTC()
	if t.IdempotencyMode == "provider_key" && (t.ValidUntil == nil || !now.Before(*t.ValidUntil)) {
		return domain.Result{Outcome: "unknown", ErrorCode: "IDEMPOTENCY_WINDOW_EXPIRED", ErrorSummary: "provider idempotency window expired"}
	}
	timeout := time.Duration(t.Config.TimeoutMS) * time.Millisecond
	until := time.Until(t.DeadlineAt)
	if until < timeout {
		timeout = until
	}
	if timeout <= 0 {
		return domain.Result{Outcome: "retryable", NotProcessed: true, ErrorCode: "DEADLINE_EXCEEDED", ErrorSummary: "task execution deadline reached"}
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, t.Request.Method, t.Request.URL, bytes.NewReader(t.Request.Body))
	if err != nil {
		return domain.Result{Outcome: "permanent_failure", ErrorCode: "INVALID_REQUEST", ErrorSummary: "invalid prepared request"}
	}
	for k, v := range t.Request.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", t.Request.ContentType)
	if t.IdempotencyMode == "provider_key" {
		req.Header.Set(t.Config.IdempotencyHeader, t.IdempotencyKey)
	}
	if err := s.authenticate(req, t.Config.Auth, t.Request.Body); err != nil {
		return domain.Result{Outcome: "permanent_failure", ErrorCode: "CREDENTIAL_UNAVAILABLE", ErrorSummary: "configured credential is unavailable"}
	}
	resp, err := s.Client.Do(req)
	if err != nil {
		r := domain.Result{Outcome: "unknown", ErrorCode: "TRANSPORT_ERROR", ErrorSummary: "external HTTP transport failed"}
		var dns *net.DNSError
		var op *net.OpError
		var cert *tls.CertificateVerificationError
		var unknown x509.UnknownAuthorityError
		switch {
		case errors.As(err, &cert) || errors.As(err, &unknown):
			r.Outcome = "permanent_failure"
			r.ErrorCode = "TLS_VERIFICATION_FAILED"
			r.ErrorSummary = "external TLS verification failed"
		case errors.As(err, &dns) || errors.As(err, &op) && op.Op == "dial":
			r.Outcome = "retryable"
			r.NotProcessed = true
			r.ErrorCode = "CONNECT_FAILED"
		case errors.Is(err, context.DeadlineExceeded):
			r.ErrorCode = "REQUEST_TIMEOUT"
			r.ErrorSummary = "external request timed out"
		}
		return r
	}
	defer resp.Body.Close()
	r := domain.Result{Outcome: "unknown", HTTPStatus: resp.StatusCode}
	body, err := io.ReadAll(io.LimitReader(resp.Body, (256<<10)+1))
	if len(body) > 256<<10 {
		r.Truncated = true
		r.ErrorCode = "RESPONSE_TOO_LARGE"
		r.ErrorSummary = "external response exceeded limit"
		return r
	}
	if err != nil {
		r.ErrorCode = "RESPONSE_READ_FAILED"
		r.ErrorSummary = "external response could not be read"
		return r
	}
	r.Summary = json.RawMessage(fmt.Sprintf(`{"http_status":%d}`, resp.StatusCode))
	success := contains(t.Config.Success.Statuses, resp.StatusCode)
	if success {
		if t.Config.Success.JSONField != "" {
			var obj map[string]any
			decoder := json.NewDecoder(bytes.NewReader(body))
			decoder.UseNumber()
			if decoder.Decode(&obj) != nil {
				r.ErrorCode = "INVALID_RESPONSE"
				r.ErrorSummary = "expected response contract missing"
				return r
			}
			var extra any
			if decoder.Decode(&extra) != io.EOF {
				r.ErrorCode = "INVALID_RESPONSE"
				r.ErrorSummary = "response contains trailing data"
				return r
			}
			value, ok := obj[t.Config.Success.JSONField]
			if !ok {
				r.ErrorCode = "INVALID_RESPONSE"
				r.ErrorSummary = "expected response field missing"
				return r
			}
			if adapter.FieldValue(value) != t.Config.Success.JSONValue {
				r.Outcome = "permanent_failure"
				r.ErrorCode = "PROVIDER_REJECTED"
				r.ErrorSummary = "provider rejected business notification"
				return r
			}
		}
		r.Outcome = "success"
		return r
	}
	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		r.ErrorCode = "UNEXPECTED_SUCCESS_STATUS"
		r.ErrorSummary = "unexpected success response"
		return r
	}
	switch {
	case resp.StatusCode >= 500:
		r.Outcome = "retryable"
		r.ErrorCode = "PROVIDER_TEMPORARY_FAILURE"
		r.ErrorSummary = "provider temporarily unavailable"
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		r.Outcome = "permanent_failure"
		r.ErrorCode = "REDIRECT_REJECTED"
		r.ErrorSummary = "provider redirect is not permitted"
	default:
		r.Outcome = "permanent_failure"
		r.ErrorCode = "PROVIDER_REJECTED"
		r.ErrorSummary = "provider returned a permanent rejection"
	}
	return r
}
func (s *Sender) authenticate(r *http.Request, a domain.Auth, body []byte) error {
	if a.Type == "" || a.Type == "none" {
		return nil
	}
	if s.Secrets == nil {
		return errors.New("missing resolver")
	}
	secret, ok := s.Secrets(a.SecretRef)
	if !ok || secret == "" {
		return errors.New("missing secret")
	}
	switch a.Type {
	case "bearer":
		r.Header.Set("Authorization", "Bearer "+secret)
	case "api_key":
		r.Header.Set(a.Header, secret)
	case "basic":
		r.SetBasicAuth(a.Username, secret)
	case "hmac":
		timestamp := strconv.FormatInt(time.Now().Unix(), 10)
		m := hmac.New(sha256.New, []byte(secret))
		m.Write([]byte(timestamp + "\n"))
		m.Write(body)
		r.Header.Set("X-Timestamp", timestamp)
		r.Header.Set("X-Signature", hex.EncodeToString(m.Sum(nil)))
	default:
		return errors.New("unsupported credential")
	}
	return nil
}
func contains(xs []int, v int) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

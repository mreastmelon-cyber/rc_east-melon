package observability

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mreastmelon-cyber/right-capital-project/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDeliveryAndRPCMetrics(t *testing.T) {
	m := New()
	m.Observe(domain.Task{DestinationID: "crm"}, domain.Result{Outcome: "success", DurationMS: 250})
	_, err := m.Interceptor(slog.New(slog.NewTextHandler(io.Discard, nil)))(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/notification.v1.NotificationService/SubmitEvent"}, func(context.Context, any) (any, error) { return nil, status.Error(codes.Unauthenticated, "bad token") })
	if status.Code(err) != codes.Unauthenticated {
		t.Fatal("interceptor changed RPC error")
	}
	r := httptest.NewRecorder()
	m.Handler().ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
	for _, want := range []string{`notification_delivery_attempts_total{destination="crm",outcome="success"} 1`, `notification_delivery_duration_seconds_sum{destination="crm"} 0.25`, `notification_rpc_requests_total{code="Unauthenticated",method="/notification.v1.NotificationService/SubmitEvent"} 1`} {
		if !strings.Contains(r.Body.String(), want) {
			t.Errorf("missing metric %s", want)
		}
	}
}

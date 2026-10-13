package observability

import (
	"context"
	"log/slog"
	"time"

	"github.com/mreastmelon-cyber/right-capital-project/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"net/http"
)

type Metrics struct {
	Registry  *prometheus.Registry
	Attempts  *prometheus.CounterVec
	Duration  *prometheus.HistogramVec
	Tasks     *prometheus.GaugeVec
	Lag       *prometheus.GaugeVec
	Heartbeat prometheus.Gauge
	RPCs      *prometheus.CounterVec
	Errors    prometheus.Counter
}

func New() *Metrics {
	m := &Metrics{Registry: prometheus.NewRegistry(), Attempts: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notification_delivery_attempts_total", Help: "Completed delivery attempts."}, []string{"destination", "outcome"}), Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "notification_delivery_duration_seconds", Help: "External HTTP duration."}, []string{"destination"}), Tasks: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "notification_tasks", Help: "Persisted task counts by destination and state."}, []string{"destination", "state"}), Lag: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "notification_oldest_due_seconds", Help: "Age of oldest due notification."}, []string{"destination"}), Heartbeat: prometheus.NewGauge(prometheus.GaugeOpts{Name: "notification_worker_heartbeat_timestamp_seconds", Help: "Last successful worker tick."}), RPCs: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notification_rpc_requests_total", Help: "RPC completions."}, []string{"method", "code"}), Errors: prometheus.NewCounter(prometheus.CounterOpts{Name: "notification_storage_errors_total", Help: "Storage failures observed by worker."})}
	m.Registry.MustRegister(m.Attempts, m.Duration, m.Tasks, m.Lag, m.Heartbeat, m.RPCs, m.Errors, prometheus.NewGoCollector())
	return m
}
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
func (m *Metrics) Observe(t domain.Task, r domain.Result) {
	m.Attempts.WithLabelValues(t.DestinationID, r.Outcome).Inc()
	m.Duration.WithLabelValues(t.DestinationID).Observe(float64(r.DurationMS) / 1000)
}
func (m *Metrics) Interceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := next(ctx, req)
		code := status.Code(err).String()
		m.RPCs.WithLabelValues(info.FullMethod, code).Inc()
		logger.InfoContext(ctx, "rpc completed", "method", info.FullMethod, "code", code, "duration_ms", time.Since(start).Milliseconds())
		return resp, err
	}
}

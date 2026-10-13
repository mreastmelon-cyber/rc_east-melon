package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mreastmelon-cyber/right-capital-project/internal/config"
	"github.com/mreastmelon-cyber/right-capital-project/internal/domain"
	"github.com/mreastmelon-cyber/right-capital-project/internal/observability"
	"github.com/mreastmelon-cyber/right-capital-project/internal/repository"
	"github.com/mreastmelon-cyber/right-capital-project/internal/transport"
)

type Runner struct {
	Store        *repository.Store
	Config       config.Config
	Sender       *transport.Sender
	Metrics      *observability.Metrics
	Logger       *slog.Logger
	PollInterval time.Duration
	MaxWorkers   int
	Active       atomic.Bool
}

func (r *Runner) Run(parent context.Context) error {
	if r.PollInterval <= 0 {
		r.PollInterval = time.Second
	}
	if r.MaxWorkers <= 0 {
		r.MaxWorkers = 8
	}
	if r.MaxWorkers > 32 {
		return domain.ErrInvalid
	}
	if r.Logger == nil {
		r.Logger = slog.Default()
	}
	lock, e := r.Store.AcquireWorker(parent)
	if e != nil {
		return e
	}
	defer lock.Close()
	r.Active.Store(true)
	defer r.Active.Store(false)
	jobs, cancel := context.WithCancel(context.WithoutCancel(parent))
	defer cancel()
	var wg sync.WaitGroup
	slots := make(chan struct{}, r.MaxWorkers)
	workerID := domain.UUID()
	ticker := time.NewTicker(r.PollInterval)
	defer ticker.Stop()
	nextMaintain := time.Time{}
	var exit error
loop:
	for {
		select {
		case <-parent.Done():
			break loop
		case <-ticker.C:
		}
		tickctx, stop := context.WithTimeout(parent, 5*time.Second)
		if !lock.Alive(tickctx) {
			stop()
			if parent.Err() != nil {
				break loop
			}
			cancel()
			exit = domain.ErrLeaseLost
			break loop
		}
		now := time.Now()
		if now.After(nextMaintain) {
			if e := r.Store.Maintain(tickctx); e != nil {
				r.storageError(e)
			}
			r.queueMetrics(tickctx)
			nextMaintain = now.Add(5 * time.Second)
		}
	dispatch:
		for {
			select {
			case slots <- struct{}{}:
			default:
				break dispatch
			}
			job, e := r.Store.Claim(tickctx, workerID, 45*time.Second)
			if e != nil {
				<-slots
				if !errors.Is(e, domain.ErrNotFound) {
					r.storageError(e)
				}
				break dispatch
			}
			wg.Add(1)
			go func(t domain.Task) { defer wg.Done(); defer func() { <-slots }(); r.execute(jobs, t) }(job)
		}
		if r.Metrics != nil {
			r.Metrics.Heartbeat.Set(float64(time.Now().Unix()))
		}
		stop()
	}
	r.Active.Store(false)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		cancel()
		<-done
	}
	return exit
}
func (r *Runner) execute(ctx context.Context, t domain.Task) {
	var result domain.Result
	if ctx.Err() != nil {
		result = domain.Result{Outcome: "retryable", NotProcessed: true, ErrorCode: "WORKER_STOPPING", ErrorSummary: "worker stopped before send"}
	} else {
		result = r.Sender.Send(ctx, t)
	}
	var e error
	if r.Metrics != nil {
		r.Metrics.Observe(t, result)
	}
	r.Logger.Info("delivery attempt completed", "task_id", t.ID, "destination", t.DestinationID, "outcome", result.Outcome, "error_code", result.ErrorCode)
	persist, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer stop()
	for i := 0; i < 3; i++ {
		e = r.Store.Finish(persist, t, result)
		if e == nil || errors.Is(e, domain.ErrLeaseLost) {
			return
		}
		if !errors.Is(e, domain.ErrUnavailable) {
			break
		}
		timer := time.NewTimer(time.Duration(50*(i+1)) * time.Millisecond)
		select {
		case <-persist.Done():
			timer.Stop()
			r.storageError(e)
			return
		case <-timer.C:
		}
	}
	r.storageError(e)
}
func (r *Runner) storageError(e error) {
	if e == nil {
		return
	}
	if r.Metrics != nil {
		r.Metrics.Errors.Inc()
	}
	r.Logger.Error("worker storage operation failed")
}
func (r *Runner) queueMetrics(ctx context.Context) {
	if r.Metrics == nil {
		return
	}
	rows, e := r.Store.DB.QueryContext(ctx, "SELECT destination_id,state,COUNT(*) FROM notification_tasks GROUP BY destination_id,state")
	if e != nil {
		r.storageError(e)
		return
	}
	r.Metrics.Tasks.Reset()
	for rows.Next() {
		var id, state string
		var count int
		if e = rows.Scan(&id, &state, &count); e != nil {
			break
		}
		r.Metrics.Tasks.WithLabelValues(id, state).Set(float64(count))
	}
	if rows.Err() != nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		r.storageError(e)
	}
	for _, d := range r.Config.Destinations {
		var lag float64
		e = r.Store.DB.QueryRowContext(ctx, "SELECT COALESCE(MAX(TIMESTAMPDIFF(MICROSECOND,next_attempt_at,UTC_TIMESTAMP(6)))/1000000,0) FROM notification_tasks WHERE destination_id=? AND state IN ('pending','retry_wait') AND next_attempt_at<=UTC_TIMESTAMP(6)", d.ID).Scan(&lag)
		if e == nil {
			r.Metrics.Lag.WithLabelValues(d.ID).Set(lag)
		} else {
			r.storageError(e)
		}
	}
}

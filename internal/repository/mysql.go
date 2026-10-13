package repository

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mreastmelon-cyber/right-capital-project/internal/domain"
	"github.com/mreastmelon-cyber/right-capital-project/migrations"
)

type Store struct{ DB *sql.DB }

func Open(ctx context.Context, dsn string) (*Store, error) {
	c, e := mysql.ParseDSN(dsn)
	if e != nil {
		return nil, e
	}
	c.ParseTime = true
	c.Loc = time.UTC
	c.MultiStatements = false
	if c.Params == nil {
		c.Params = map[string]string{}
	}
	c.Params["time_zone"] = "'+00:00'"
	c.Timeout = 5 * time.Second
	c.ReadTimeout = 10 * time.Second
	c.WriteTimeout = 10 * time.Second
	db, e := sql.Open("mysql", c.FormatDSN())
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(5 * time.Minute)
	if e = db.PingContext(ctx); e != nil {
		db.Close()
		return nil, e
	}
	return &Store{db}, nil
}
func (s *Store) Migrate(ctx context.Context) error {
	c, e := s.DB.Conn(ctx)
	if e != nil {
		return e
	}
	defer c.Close()
	const lock = "CONCAT('notif:migrate:',LEFT(SHA2(DATABASE(),256),48))"
	var ok int
	if e = c.QueryRowContext(ctx, "SELECT GET_LOCK("+lock+",10)").Scan(&ok); e != nil {
		return e
	}
	if ok != 1 {
		return domain.ErrPrecondition
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c.ExecContext(ctx, "SELECT RELEASE_LOCK("+lock+")")
	}()
	if _, e = c.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS schema_migrations (version INT UNSIGNED PRIMARY KEY, checksum BINARY(32) NOT NULL, applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)) ENGINE=InnoDB"); e != nil {
		return e
	}
	h := sha256.Sum256([]byte(migrations.Initial))
	var previous []byte
	e = c.QueryRowContext(ctx, "SELECT checksum FROM schema_migrations WHERE version=1").Scan(&previous)
	if e == nil {
		if string(previous) != string(h[:]) {
			return errors.New("migration checksum mismatch")
		}
		return nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	for _, stmt := range statements(migrations.Initial) {
		if _, e = c.ExecContext(ctx, stmt); e != nil {
			return fmt.Errorf("initial migration failed; use a fresh database or inspect partial DDL: %w", e)
		}
	}
	_, e = c.ExecContext(ctx, "INSERT INTO schema_migrations(version,checksum) VALUES(1,?)", h[:])
	return e
}
func statements(s string) []string {
	var out []string
	quote := byte(0)
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == quote {
				if i+1 < len(s) && s[i+1] == quote {
					i++
				} else {
					quote = 0
				}
			}
			continue
		}
		if c == '\'' || c == '`' {
			quote = c
		} else if c == ';' {
			if v := strings.TrimSpace(s[start:i]); v != "" {
				out = append(out, v)
			}
			start = i + 1
		}
	}
	if v := strings.TrimSpace(s[start:]); v != "" {
		out = append(out, v)
	}
	return out
}
func dbError(e error) error {
	if e == nil {
		return nil
	}
	if errors.Is(e, sql.ErrNoRows) {
		return domain.ErrNotFound
	}
	if errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
		return e
	}
	return fmt.Errorf("%w", domain.ErrUnavailable)
}
func duplicate(e error) bool { var m *mysql.MySQLError; return errors.As(e, &m) && m.Number == 1062 }
func begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	return db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}
func clock(ctx context.Context, tx *sql.Tx) (time.Time, error) {
	var n time.Time
	e := tx.QueryRowContext(ctx, "SELECT UTC_TIMESTAMP(6)").Scan(&n)
	return n, e
}
func encode(v any) []byte {
	b, e := json.Marshal(v)
	if e != nil {
		panic(e)
	}
	return b
}
func nullable(v string) any {
	if v == "" {
		return nil
	}
	return v
}
func nullableStatus(v int) any {
	if v == 0 {
		return nil
	}
	return v
}
func nullableJSON(v json.RawMessage) any {
	if len(v) == 0 {
		return nil
	}
	return []byte(v)
}

const eventColumns = "id,source_system,source_event_id,event_type,schema_version,occurred_at,received_at,payload,payload_hash,hash_version,routing_config_version"

type scanner interface{ Scan(...any) error }

func scanEvent(r scanner) (domain.Event, error) {
	var v domain.Event
	var hash []byte
	e := r.Scan(&v.ID, &v.Source, &v.SourceEventID, &v.Type, &v.SchemaVersion, &v.OccurredAt, &v.ReceivedAt, &v.Payload, &hash, &v.HashVersion, &v.ConfigVersion)
	if e == nil && len(hash) != 32 {
		return v, domain.ErrUnavailable
	}
	copy(v.Hash[:], hash)
	return v, dbError(e)
}
func (s *Store) GetEvent(ctx context.Context, id uint64) (domain.Event, error) {
	return scanEvent(s.DB.QueryRowContext(ctx, "SELECT "+eventColumns+" FROM notification_events WHERE id=?", id))
}
func (s *Store) FindEvent(ctx context.Context, source, id string) (domain.Event, error) {
	return scanEvent(s.DB.QueryRowContext(ctx, "SELECT "+eventColumns+" FROM notification_events WHERE source_system=? AND source_event_id=?", source, id))
}
func (s *Store) CreateEvent(ctx context.Context, v domain.Event, tasks []domain.Task) (domain.Event, []domain.Task, bool, error) {
	tx, e := begin(ctx, s.DB)
	if e != nil {
		return v, nil, false, dbError(e)
	}
	defer tx.Rollback()
	now, e := clock(ctx, tx)
	if e != nil {
		return v, nil, false, dbError(e)
	}
	result, e := tx.ExecContext(ctx, "INSERT INTO notification_events(source_system,source_event_id,event_type,schema_version,occurred_at,payload,payload_hash,hash_version,routing_config_version) VALUES(?,?,?,?,?,?,?,?,?)", v.Source, v.SourceEventID, v.Type, v.SchemaVersion, v.OccurredAt, v.Payload, v.Hash[:], v.HashVersion, v.ConfigVersion)
	if duplicate(e) {
		tx.Rollback()
		old, err := s.FindEvent(ctx, v.Source, v.SourceEventID)
		if err != nil {
			return v, nil, false, err
		}
		if old.Hash != v.Hash || old.HashVersion != v.HashVersion {
			return old, nil, false, domain.ErrConflict
		}
		jobs, err := s.EventTasks(ctx, old.ID)
		return old, jobs, true, err
	}
	if e != nil {
		return v, nil, false, dbError(e)
	}
	id, e := result.LastInsertId()
	if e != nil {
		return v, nil, false, dbError(e)
	}
	v.ID = uint64(id)
	v.ReceivedAt = now
	for i := range tasks {
		t := &tasks[i]
		t.EventID = v.ID
		t.State = domain.Pending
		t.RunNo = 1
		t.CreatedAt = now
		t.UpdatedAt = now
		t.NextAttemptAt = &now
		t.DeadlineAt = now.Add(time.Duration(t.Config.DeadlineSeconds) * time.Second)
		t.MaxAttempts = t.Config.MaxAttempts
		b := t.Request.Body
		if b == nil {
			b = []byte{}
		}
		r, err := tx.ExecContext(ctx, `INSERT INTO notification_tasks(event_id,destination_id,adapter_key,config_version,config_snapshot,request_method,request_url,request_headers,request_content_type,request_body,idempotency_mode,idempotency_key,idempotency_retention_seconds,max_attempts,retry_policy,deadline_at,next_attempt_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, t.EventID, t.DestinationID, t.AdapterKey, t.ConfigVersion, encode(t.Config), t.Request.Method, t.Request.URL, encode(t.Request.Headers), t.Request.ContentType, b, t.IdempotencyMode, t.IdempotencyKey, t.RetentionSeconds, t.MaxAttempts, encode(t.Policy), t.DeadlineAt, now)
		if err != nil {
			return v, nil, false, dbError(err)
		}
		id, err := r.LastInsertId()
		if err != nil {
			return v, nil, false, dbError(err)
		}
		t.ID = uint64(id)
	}
	if e = tx.Commit(); e != nil {
		return v, nil, false, dbError(e)
	}
	return v, tasks, false, nil
}

const taskColumns = "id,event_id,destination_id,adapter_key,config_version,config_snapshot,request_method,request_url,request_headers,request_content_type,request_body,idempotency_mode,idempotency_key,idempotency_retention_seconds,first_attempt_at,idempotency_valid_until,state,attempt_count,run_no,run_attempt_count,max_attempts,retry_policy,deadline_at,next_attempt_at,lease_token,lease_until,last_http_status,last_provider_code,last_error_code,last_error_summary,completed_at,closed_at,closed_by,created_at,updated_at"

func scanTask(r scanner) (domain.Task, error) {
	var t domain.Task
	var config, headers, policy []byte
	var lease, provider, code, summary, closed sql.NullString
	var httpStatus sql.NullInt64
	e := r.Scan(&t.ID, &t.EventID, &t.DestinationID, &t.AdapterKey, &t.ConfigVersion, &config, &t.Request.Method, &t.Request.URL, &headers, &t.Request.ContentType, &t.Request.Body, &t.IdempotencyMode, &t.IdempotencyKey, &t.RetentionSeconds, &t.FirstAttemptAt, &t.ValidUntil, &t.State, &t.AttemptCount, &t.RunNo, &t.RunAttemptCount, &t.MaxAttempts, &policy, &t.DeadlineAt, &t.NextAttemptAt, &lease, &t.LeaseUntil, &httpStatus, &provider, &code, &summary, &t.CompletedAt, &t.ClosedAt, &closed, &t.CreatedAt, &t.UpdatedAt)
	if e != nil {
		return t, dbError(e)
	}
	t.LeaseToken = lease.String
	t.LastHTTPStatus = int(httpStatus.Int64)
	t.LastProviderCode = provider.String
	t.LastErrorCode = code.String
	t.LastErrorSummary = summary.String
	t.ClosedBy = closed.String
	if json.Unmarshal(config, &t.Config) != nil || json.Unmarshal(headers, &t.Request.Headers) != nil || json.Unmarshal(policy, &t.Policy) != nil {
		return t, domain.ErrUnavailable
	}
	return t, nil
}
func (s *Store) GetTask(ctx context.Context, id uint64) (domain.Task, error) {
	return scanTask(s.DB.QueryRowContext(ctx, "SELECT "+taskColumns+" FROM notification_tasks WHERE id=?", id))
}
func taskRows(rows *sql.Rows) ([]domain.Task, error) {
	defer rows.Close()
	out := []domain.Task{}
	for rows.Next() {
		t, e := scanTask(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, t)
	}
	return out, dbError(rows.Err())
}
func (s *Store) EventTasks(ctx context.Context, id uint64) ([]domain.Task, error) {
	rows, e := s.DB.QueryContext(ctx, "SELECT "+taskColumns+" FROM notification_tasks WHERE event_id=? ORDER BY id", id)
	if e != nil {
		return nil, dbError(e)
	}
	return taskRows(rows)
}
func (s *Store) ListTasks(ctx context.Context, f domain.ListFilter) ([]domain.Task, error) {
	q := "SELECT " + taskColumns + " FROM notification_tasks WHERE id>?"
	args := []any{f.AfterID}
	if f.Destination != "" {
		q += " AND destination_id=?"
		args = append(args, f.Destination)
	}
	if f.State != "" {
		q += " AND state=?"
		args = append(args, f.State)
	}
	q += " ORDER BY id LIMIT ?"
	args = append(args, f.Limit)
	rows, e := s.DB.QueryContext(ctx, q, args...)
	if e != nil {
		return nil, dbError(e)
	}
	return taskRows(rows)
}
func (s *Store) Claim(ctx context.Context, worker string, lease time.Duration) (domain.Task, error) {
	tx, e := begin(ctx, s.DB)
	if e != nil {
		return domain.Task{}, dbError(e)
	}
	defer tx.Rollback()
	now, e := clock(ctx, tx)
	if e != nil {
		return domain.Task{}, dbError(e)
	}
	t, e := scanTask(tx.QueryRowContext(ctx, "SELECT "+taskColumns+" FROM notification_tasks WHERE state IN ('pending','retry_wait') AND next_attempt_at<=? AND deadline_at>? AND run_attempt_count<max_attempts ORDER BY next_attempt_at,id LIMIT 1 FOR UPDATE SKIP LOCKED", now, now))
	if e != nil {
		return t, e
	}
	t.State = domain.Running
	t.NextAttemptAt = nil
	t.AttemptCount++
	t.RunAttemptCount++
	t.LeaseToken = domain.UUID()
	t.LeaseUntil = domain.Ptr(now.Add(lease))
	t.CompletedAt = nil
	if t.FirstAttemptAt == nil {
		t.FirstAttemptAt = &now
		if t.IdempotencyMode == "provider_key" {
			t.ValidUntil = domain.Ptr(now.Add(time.Duration(*t.RetentionSeconds) * time.Second))
		}
	}
	if e = writeTask(ctx, tx, t); e != nil {
		return t, dbError(e)
	}
	if _, e = tx.ExecContext(ctx, "INSERT INTO notification_attempts(task_id,attempt_no,run_no,lease_token,worker_id,started_at) VALUES(?,?,?,?,?,?)", t.ID, t.AttemptCount, t.RunNo, t.LeaseToken, worker, now); e != nil {
		return t, dbError(e)
	}
	return t, dbError(tx.Commit())
}
func writeTask(ctx context.Context, tx *sql.Tx, t domain.Task) error {
	_, e := tx.ExecContext(ctx, `UPDATE notification_tasks SET state=?,attempt_count=?,run_no=?,run_attempt_count=?,deadline_at=?,next_attempt_at=?,lease_token=?,lease_until=?,first_attempt_at=?,idempotency_valid_until=?,last_http_status=?,last_provider_code=?,last_error_code=?,last_error_summary=?,completed_at=?,closed_at=?,closed_by=? WHERE id=?`, t.State, t.AttemptCount, t.RunNo, t.RunAttemptCount, t.DeadlineAt, t.NextAttemptAt, nullable(t.LeaseToken), t.LeaseUntil, t.FirstAttemptAt, t.ValidUntil, nullableStatus(t.LastHTTPStatus), nullable(t.LastProviderCode), nullable(t.LastErrorCode), nullable(t.LastErrorSummary), t.CompletedAt, t.ClosedAt, nullable(t.ClosedBy), t.ID)
	return e
}
func (s *Store) Finish(ctx context.Context, claimed domain.Task, r domain.Result) error {
	tx, e := begin(ctx, s.DB)
	if e != nil {
		return dbError(e)
	}
	defer tx.Rollback()
	t, e := scanTask(tx.QueryRowContext(ctx, "SELECT "+taskColumns+" FROM notification_tasks WHERE id=? FOR UPDATE", claimed.ID))
	if e != nil {
		return e
	}
	now, e := clock(ctx, tx)
	if e != nil {
		return dbError(e)
	}
	// A response arriving after expiration cannot renew its own lease, even
	// when the recovery poll has not yet run.
	if t.State == domain.Running && t.LeaseToken == claimed.LeaseToken && t.LeaseUntil != nil && !now.Before(*t.LeaseUntil) {
		if _, e = tx.ExecContext(ctx, "UPDATE notification_attempts SET outcome='unknown',finished_at=?,error_code='LEASE_EXPIRED',error_summary='worker lease expired' WHERE task_id=? AND lease_token=? AND outcome='started'", now, t.ID, t.LeaseToken); e != nil {
			return dbError(e)
		}
		t = domain.Complete(t, domain.Result{Outcome: "unknown", ErrorCode: "LEASE_EXPIRED", ErrorSummary: "worker lease expired"}, now)
		if e = writeTask(ctx, tx, t); e != nil {
			return dbError(e)
		}
	}
	if t.State != domain.Running || t.LeaseToken != claimed.LeaseToken {
		summary := encode(map[string]any{"outcome": r.Outcome, "http_status": r.HTTPStatus, "error_code": r.ErrorCode})
		if _, e = tx.ExecContext(ctx, "UPDATE notification_attempts SET late_result_at=?,late_result_summary=? WHERE task_id=? AND lease_token=? AND outcome='unknown' AND late_result_at IS NULL", now, summary, t.ID, claimed.LeaseToken); e != nil {
			return dbError(e)
		}
		if e = tx.Commit(); e != nil {
			return dbError(e)
		}
		return domain.ErrLeaseLost
	}
	updated := domain.Complete(t, r, now)
	if _, e = tx.ExecContext(ctx, `UPDATE notification_attempts SET outcome=?,finished_at=?,duration_ms=?,http_status=?,provider_code=?,error_code=?,error_summary=?,response_summary=?,response_truncated=? WHERE task_id=? AND lease_token=? AND outcome='started'`, r.Outcome, now, r.DurationMS, nullableStatus(r.HTTPStatus), nullable(r.ProviderCode), nullable(r.ErrorCode), nullable(r.ErrorSummary), nullableJSON(r.Summary), r.Truncated, t.ID, t.LeaseToken); e != nil {
		return dbError(e)
	}
	if e = writeTask(ctx, tx, updated); e != nil {
		return dbError(e)
	}
	return dbError(tx.Commit())
}
func (s *Store) Maintain(ctx context.Context) error {
	rows, e := s.DB.QueryContext(ctx, "SELECT id FROM notification_tasks WHERE (state='running' AND lease_until<=UTC_TIMESTAMP(6)) OR (state IN ('pending','retry_wait') AND (deadline_at<=UTC_TIMESTAMP(6) OR run_attempt_count>=max_attempts)) ORDER BY id LIMIT 100")
	if e != nil {
		return dbError(e)
	}
	var ids []uint64
	for rows.Next() {
		var id uint64
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return dbError(e)
		}
		ids = append(ids, id)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return dbError(e)
	}
	for _, id := range ids {
		if e = s.recoverOne(ctx, id); e != nil {
			return e
		}
	}
	return nil
}
func (s *Store) recoverOne(ctx context.Context, id uint64) error {
	tx, e := begin(ctx, s.DB)
	if e != nil {
		return dbError(e)
	}
	defer tx.Rollback()
	t, e := scanTask(tx.QueryRowContext(ctx, "SELECT "+taskColumns+" FROM notification_tasks WHERE id=? FOR UPDATE", id))
	if e != nil {
		return e
	}
	now, e := clock(ctx, tx)
	if e != nil {
		return dbError(e)
	}
	switch {
	case t.State == domain.Running && t.LeaseUntil != nil && !now.Before(*t.LeaseUntil):
		if _, e = tx.ExecContext(ctx, "UPDATE notification_attempts SET outcome='unknown',finished_at=?,error_code='LEASE_EXPIRED',error_summary='worker lease expired' WHERE task_id=? AND lease_token=? AND outcome='started'", now, t.ID, t.LeaseToken); e != nil {
			return dbError(e)
		}
		t = domain.Complete(t, domain.Result{Outcome: "unknown", ErrorCode: "LEASE_EXPIRED", ErrorSummary: "worker lease expired"}, now)
	case (t.State == domain.Pending || t.State == domain.RetryWait) && (!now.Before(t.DeadlineAt) || t.RunAttemptCount >= t.MaxAttempts):
		t.State = domain.Failed
		t.NextAttemptAt = nil
		t.CompletedAt = &now
		t.LastErrorCode = "RETRY_BUDGET_EXHAUSTED"
		t.LastErrorSummary = "automatic delivery budget exhausted"
	default:
		return nil
	}
	if e = writeTask(ctx, tx, t); e != nil {
		return dbError(e)
	}
	return dbError(tx.Commit())
}
func (s *Store) Attempts(ctx context.Context, id, after uint64, limit int) ([]domain.Attempt, error) {
	rows, e := s.DB.QueryContext(ctx, "SELECT id,task_id,attempt_no,run_no,lease_token,worker_id,started_at,finished_at,duration_ms,outcome,http_status,provider_code,error_code,error_summary,response_summary,response_truncated,late_result_at,late_result_summary FROM notification_attempts WHERE task_id=? AND id>? ORDER BY id LIMIT ?", id, after, limit)
	if e != nil {
		return nil, dbError(e)
	}
	defer rows.Close()
	out := []domain.Attempt{}
	for rows.Next() {
		var a domain.Attempt
		var response, late []byte
		var status sql.NullInt64
		var code, summary, provider sql.NullString
		if e = rows.Scan(&a.ID, &a.TaskID, &a.AttemptNo, &a.RunNo, &a.LeaseToken, &a.WorkerID, &a.StartedAt, &a.FinishedAt, &a.DurationMS, &a.Outcome, &status, &provider, &code, &summary, &response, &a.Truncated, &a.LateResultAt, &late); e != nil {
			return nil, dbError(e)
		}
		a.HTTPStatus = int(status.Int64)
		a.ProviderCode = provider.String
		a.ErrorCode = code.String
		a.ErrorSummary = summary.String
		a.ResponseSummary = response
		a.LateResult = late
		out = append(out, a)
	}
	return out, dbError(rows.Err())
}

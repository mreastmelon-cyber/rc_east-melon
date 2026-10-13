package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/mreastmelon-cyber/right-capital-project/internal/domain"
)

type AdminResult struct {
	Task *domain.Task `json:"task,omitempty"`
}
type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readOperation(ctx context.Context, q queryer, op domain.Operation, lock bool) (AdminResult, error) {
	var hash, data []byte
	sqltext := "SELECT request_hash,result_summary FROM notification_operations WHERE operator_id=? AND operation_id=?"
	if lock {
		sqltext += " FOR UPDATE"
	}
	e := q.QueryRowContext(ctx, sqltext, op.Operator, op.ID).Scan(&hash, &data)
	if e != nil {
		return AdminResult{}, dbError(e)
	}
	if string(hash) != string(op.Hash[:]) {
		return AdminResult{}, domain.ErrConflict
	}
	var result AdminResult
	if json.Unmarshal(data, &result) != nil {
		return result, domain.ErrUnavailable
	}
	return result, nil
}
func recordOperation(ctx context.Context, tx *sql.Tx, op domain.Operation, id any, destination string, before, after any, result AdminResult) error {
	_, e := tx.ExecContext(ctx, `INSERT INTO notification_operations(operator_id,operation_id,operation_type,task_id,destination_id,request_hash,reason,request_summary,before_snapshot,after_snapshot,result_summary) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, op.Operator, op.ID, op.Type, id, destination, op.Hash[:], op.Reason, op.Request, encode(before), encode(after), encode(result))
	return e
}
func (s *Store) ChangeTask(ctx context.Context, id uint64, op domain.Operation, change func(*domain.Task, time.Time) error) (domain.Task, error) {
	cached, e := readOperation(ctx, s.DB, op, false)
	if e == nil {
		if cached.Task == nil {
			return domain.Task{}, domain.ErrConflict
		}
		return *cached.Task, nil
	}
	if !errors.Is(e, domain.ErrNotFound) {
		return domain.Task{}, e
	}
	tx, e := begin(ctx, s.DB)
	if e != nil {
		return domain.Task{}, dbError(e)
	}
	defer tx.Rollback()
	t, e := scanTask(tx.QueryRowContext(ctx, "SELECT "+taskColumns+" FROM notification_tasks WHERE id=? FOR UPDATE", id))
	if e != nil {
		return t, e
	}
	cached, e = readOperation(ctx, tx, op, true)
	if e == nil {
		if cached.Task == nil {
			return t, domain.ErrConflict
		}
		return *cached.Task, nil
	}
	if !errors.Is(e, domain.ErrNotFound) {
		return t, e
	}
	before := t
	now, e := clock(ctx, tx)
	if e != nil {
		return t, dbError(e)
	}
	if e = change(&t, now); e != nil {
		return t, e
	}
	if e = writeTask(ctx, tx, t); e != nil {
		return t, dbError(e)
	}
	if e = recordOperation(ctx, tx, op, t.ID, t.DestinationID, before, t, AdminResult{Task: &t}); e != nil {
		if duplicate(e) {
			tx.Rollback()
			cached, e = readOperation(ctx, s.DB, op, false)
			if e == nil && cached.Task != nil {
				return *cached.Task, nil
			}
			return t, e
		}
		return t, dbError(e)
	}
	return t, dbError(tx.Commit())
}

type WorkerLock struct{ conn *sql.Conn }

const workerLockName = "CONCAT('notif:worker:',LEFT(SHA2(DATABASE(),256),48))"

func (s *Store) AcquireWorker(ctx context.Context) (*WorkerLock, error) {
	c, e := s.DB.Conn(ctx)
	if e != nil {
		return nil, dbError(e)
	}
	var ok int
	e = c.QueryRowContext(ctx, "SELECT GET_LOCK("+workerLockName+",0)").Scan(&ok)
	if e != nil || ok != 1 {
		c.Close()
		if e != nil {
			return nil, dbError(e)
		}
		return nil, domain.ErrPrecondition
	}
	return &WorkerLock{c}, nil
}
func (l *WorkerLock) Alive(ctx context.Context) bool {
	var ok sql.NullBool
	return l.conn.QueryRowContext(ctx, "SELECT IS_USED_LOCK("+workerLockName+")=CONNECTION_ID()").Scan(&ok) == nil && ok.Valid && ok.Bool
}
func (l *WorkerLock) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	l.conn.ExecContext(ctx, "SELECT RELEASE_LOCK("+workerLockName+")")
	l.conn.Close()
}

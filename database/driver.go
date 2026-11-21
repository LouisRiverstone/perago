package database

import (
	"context"
	"database/sql"
	"strconv"
	"time"

	"github.com/LouisRiverstone/perago"
)

// DriverOptions configures NewDriver.
type DriverOptions struct {
	// Table is the jobs table ("jobs").
	Table string
	// RetryAfter is how long a reserved job may run before another worker takes it again
	// (retry_after, default 90s). Keep it longer than the longest job timeout.
	RetryAfter time.Duration
	// Now replaces the clock (tests).
	Now func() time.Time
}

// Driver is the database queue driver.
type Driver struct {
	db         *DB
	sql        sqlBuilder
	retryAfter time.Duration
	now        func() time.Time
}

// NewDriver returns the database queue driver on db.
func NewDriver(db *DB, o DriverOptions) *Driver {
	if o.RetryAfter <= 0 {
		o.RetryAfter = 90 * time.Second
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Driver{db: db, sql: sqlBuilder{dialect: db.dialect, table: or(o.Table, JobsTable)}, retryAfter: o.RetryAfter, now: o.Now}
}

func (d *Driver) exec(ctx context.Context, query string, args ...any) (int64, error) {
	return d.db.exec(ctx, perago.DriverFailed, query, args...)
}

func (d *Driver) Push(ctx context.Context, m perago.Message) (string, error) {
	now := d.now()
	q := d.sql.insert("queue", "payload", "attempts", "available_at", "created_at")
	if _, err := d.exec(ctx, q, m.Queue, string(m.Payload), 0, now.Add(m.Delay).Unix(), now.Unix()); err != nil {
		return "", err
	}
	return m.ID, nil
}

// PushBulk inserts the messages in one transaction.
func (d *Driver) PushBulk(ctx context.Context, ms []perago.Message) ([]string, error) {
	ids := make([]string, 0, len(ms))
	err := d.db.transaction(ctx, func(tx *DB) error {
		inner := &Driver{db: tx, sql: d.sql, retryAfter: d.retryAfter, now: d.now}
		for _, m := range ms {
			id, err := inner.Push(ctx, m)
			if err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// available is the condition of a job a worker may take: pending and due, or reserved for
// longer than retry_after.
func (d *Driver) available() string {
	q := d.sql.q
	return "((" + q("reserved_at") + " IS NULL AND " + q("available_at") + " <= ?) OR " + q("reserved_at") + " <= ?)"
}

const popCandidates = 5

// Pop reserves the oldest available job. It selects a few candidates, then claims one with a
// conditional UPDATE that only succeeds while the row is still available and unchanged, so two
// workers never take the same job, on every dialect, without SKIP LOCKED.
func (d *Driver) Pop(ctx context.Context, queue string) (perago.Reserved, error) {
	q := d.sql.q
	now := d.now().Unix()
	expired := now - int64(d.retryAfter/time.Second)
	sel := d.sql.limit(d.sql.cols("id", "attempts")+" FROM "+d.sql.t()+" WHERE "+q("queue")+" = ? AND "+d.available()+" ORDER BY "+q("id")+" ASC", popCandidates)
	rows, err := d.db.query(ctx, sel, queue, now, expired)
	if err != nil {
		return nil, perago.Wrap(perago.PopFailed, err, "select jobs")
	}
	type candidate struct {
		id       int64
		attempts int
	}
	var cands []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.id, &c.attempts); err != nil {
			_ = rows.Close()
			return nil, perago.Wrap(perago.PopFailed, err, "scan job")
		}
		cands = append(cands, c)
	}
	if err := rows.Close(); err != nil {
		return nil, perago.Wrap(perago.PopFailed, err, "select jobs")
	}
	claim := "UPDATE " + d.sql.t() + " SET " + q("reserved_at") + " = ?, " + q("attempts") + " = " + q("attempts") + " + 1 WHERE " +
		q("id") + " = ? AND " + q("attempts") + " = ? AND " + d.available()
	for _, c := range cands {
		n, err := d.exec(ctx, claim, now, c.id, c.attempts, now, expired)
		if err != nil {
			return nil, err
		}
		if n != 1 {
			continue
		}
		var payload string
		row := d.db.queryRow(ctx, "SELECT "+q("payload")+" FROM "+d.sql.t()+" WHERE "+q("id")+" = ?", c.id)
		if err := row.Scan(&payload); err != nil {
			return nil, perago.Wrap(perago.PopFailed, err, "load job %d", c.id)
		}
		return &reserved{d: d, id: c.id, queue: queue, payload: []byte(payload), attempts: c.attempts + 1}, nil
	}
	return nil, nil
}

func (d *Driver) count(ctx context.Context, where string, args ...any) (int64, error) {
	var n int64
	row := d.db.queryRow(ctx, "SELECT COUNT(*) FROM "+d.sql.t()+" WHERE "+where, args...)
	if err := row.Scan(&n); err != nil {
		return 0, perago.Wrap(perago.DriverFailed, err, "count jobs")
	}
	return n, nil
}

func (d *Driver) Size(ctx context.Context, queue string) (int64, error) {
	return d.count(ctx, d.sql.q("queue")+" = ?", queue)
}

func (d *Driver) Clear(ctx context.Context, queue string) (int64, error) {
	return d.exec(ctx, "DELETE FROM "+d.sql.t()+" WHERE "+d.sql.q("queue")+" = ?", queue)
}

func (d *Driver) PendingSize(ctx context.Context, queue string) (int64, error) {
	q := d.sql.q
	return d.count(ctx, q("queue")+" = ? AND "+q("reserved_at")+" IS NULL AND "+q("available_at")+" <= ?", queue, d.now().Unix())
}

func (d *Driver) DelayedSize(ctx context.Context, queue string) (int64, error) {
	q := d.sql.q
	return d.count(ctx, q("queue")+" = ? AND "+q("reserved_at")+" IS NULL AND "+q("available_at")+" > ?", queue, d.now().Unix())
}

func (d *Driver) ReservedSize(ctx context.Context, queue string) (int64, error) {
	q := d.sql.q
	return d.count(ctx, q("queue")+" = ? AND "+q("reserved_at")+" IS NOT NULL", queue)
}

func (d *Driver) OldestPending(ctx context.Context, queue string) (time.Time, error) {
	q := d.sql.q
	var ts sql.NullInt64
	row := d.db.queryRow(ctx, "SELECT MIN("+q("created_at")+") FROM "+d.sql.t()+" WHERE "+q("queue")+" = ? AND "+q("reserved_at")+" IS NULL", queue)
	if err := row.Scan(&ts); err != nil {
		return time.Time{}, perago.Wrap(perago.DriverFailed, err, "oldest pending job")
	}
	if !ts.Valid {
		return time.Time{}, nil
	}
	return time.Unix(ts.Int64, 0), nil
}

type reserved struct {
	d        *Driver
	id       int64
	queue    string
	payload  []byte
	attempts int
}

func (r *reserved) ID() string      { return strconv.FormatInt(r.id, 10) }
func (r *reserved) Queue() string   { return r.queue }
func (r *reserved) Payload() []byte { return r.payload }
func (r *reserved) Attempts() int   { return r.attempts }

func (r *reserved) Delete(ctx context.Context) error {
	_, err := r.d.exec(ctx, "DELETE FROM "+r.d.sql.t()+" WHERE "+r.d.sql.q("id")+" = ?", r.id)
	return err
}

func (r *reserved) Release(ctx context.Context, delay time.Duration) error {
	q := r.d.sql.q
	_, err := r.d.exec(ctx, "UPDATE "+r.d.sql.t()+" SET "+q("reserved_at")+" = NULL, "+q("available_at")+" = ? WHERE "+q("id")+" = ?",
		r.d.now().Add(delay).Unix(), r.id)
	return err
}

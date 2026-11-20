package database

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"time"

	"github.com/LouisRiverstone/migro"
	"github.com/LouisRiverstone/perago"
)

func run(ctx context.Context, db *migro.DB, code perago.Code, query string, args ...any) (int64, error) {
	res := db.Gorm.WithContext(ctx).Exec(query, args...)
	if res.Error != nil {
		return 0, perago.Wrap(code, res.Error, "database")
	}
	return res.RowsAffected, nil
}

// FailedJobStore keeps failed jobs in the failed_jobs table.
type FailedJobStore struct {
	db  *migro.DB
	sql sqlBuilder
	now func() time.Time
}

// NewFailedJobStore returns the database failed job store ("" uses failed_jobs).
func NewFailedJobStore(db *migro.DB, table string) *FailedJobStore {
	return &FailedJobStore{db: db, sql: sqlBuilder{db: db, table: or(table, FailedTable)}, now: time.Now}
}

var failedCols = []string{"uuid", "connection", "queue", "payload", "exception", "failed_at"}

func (s *FailedJobStore) Log(ctx context.Context, f perago.FailedJob) error {
	return s.db.Transaction(ctx, func(tx *migro.DB) error {
		if _, err := run(ctx, tx, perago.StoreFailed, "DELETE FROM "+s.sql.t()+" WHERE "+s.sql.q("uuid")+" = ?", f.ID); err != nil {
			return err
		}
		_, err := run(ctx, tx, perago.StoreFailed, s.sql.insert(failedCols...), f.ID, f.Connection, f.Queue, string(f.Payload), f.Exception, f.FailedAt.Unix())
		return err
	})
}

func (s *FailedJobStore) query(ctx context.Context, where string, args ...any) ([]perago.FailedJob, error) {
	q := "SELECT " + s.sql.cols(failedCols...) + " FROM " + s.sql.t()
	if where != "" {
		q += " WHERE " + where
	}
	q += " ORDER BY " + s.sql.q("failed_at") + " DESC"
	rows, err := s.db.Gorm.WithContext(ctx).Raw(q, args...).Rows()
	if err != nil {
		return nil, perago.Wrap(perago.StoreFailed, err, "list failed jobs")
	}
	defer rows.Close()
	var out []perago.FailedJob
	for rows.Next() {
		var f perago.FailedJob
		var payload string
		var at int64
		if err := rows.Scan(&f.ID, &f.Connection, &f.Queue, &payload, &f.Exception, &at); err != nil {
			return nil, perago.Wrap(perago.StoreFailed, err, "scan failed job")
		}
		f.Payload, f.FailedAt = []byte(payload), time.Unix(at, 0)
		out = append(out, f)
	}
	if err := rows.Err(); err != nil {
		return nil, perago.Wrap(perago.StoreFailed, err, "list failed jobs")
	}
	return out, nil
}

func (s *FailedJobStore) All(ctx context.Context) ([]perago.FailedJob, error) {
	return s.query(ctx, "")
}

func (s *FailedJobStore) Find(ctx context.Context, id string) (*perago.FailedJob, error) {
	fs, err := s.query(ctx, s.sql.q("uuid")+" = ?", id)
	if err != nil {
		return nil, err
	}
	if len(fs) == 0 {
		return nil, perago.Newf(perago.FailedJobNotFound, "failed job %s not found", id)
	}
	return &fs[0], nil
}

func (s *FailedJobStore) IDs(ctx context.Context, queue string) ([]string, error) {
	q := "SELECT " + s.sql.q("uuid") + " FROM " + s.sql.t()
	var args []any
	if queue != "" {
		q += " WHERE " + s.sql.q("queue") + " = ?"
		args = append(args, queue)
	}
	rows, err := s.db.Gorm.WithContext(ctx).Raw(q+" ORDER BY "+s.sql.q("failed_at")+" ASC", args...).Rows()
	if err != nil {
		return nil, perago.Wrap(perago.StoreFailed, err, "failed job ids")
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, perago.Wrap(perago.StoreFailed, err, "scan failed job id")
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *FailedJobStore) Forget(ctx context.Context, id string) (bool, error) {
	n, err := run(ctx, s.db, perago.StoreFailed, "DELETE FROM "+s.sql.t()+" WHERE "+s.sql.q("uuid")+" = ?", id)
	return n > 0, err
}

func (s *FailedJobStore) Flush(ctx context.Context, olderThan time.Duration) (int64, error) {
	if olderThan <= 0 {
		return run(ctx, s.db, perago.StoreFailed, "DELETE FROM "+s.sql.t())
	}
	return s.Prune(ctx, s.now().Add(-olderThan))
}

func (s *FailedJobStore) Prune(ctx context.Context, before time.Time) (int64, error) {
	return run(ctx, s.db, perago.StoreFailed, "DELETE FROM "+s.sql.t()+" WHERE "+s.sql.q("failed_at")+" < ?", before.Unix())
}

// BatchStore keeps batches in the job_batches table.
type BatchStore struct {
	db  *migro.DB
	sql sqlBuilder
}

// NewBatchStore returns the database batch store ("" uses job_batches).
func NewBatchStore(db *migro.DB, table string) *BatchStore {
	return &BatchStore{db: db, sql: sqlBuilder{db: db, table: or(table, BatchesTable)}}
}

var batchCols = []string{"id", "name", "total_jobs", "pending_jobs", "failed_jobs", "failed_job_ids", "options", "cancelled_at", "created_at", "finished_at"}

func nullUnix(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.Unix()
}

func (s *BatchStore) Store(ctx context.Context, b *perago.Batch) error {
	opts, err := json.Marshal(b.Options)
	if err != nil {
		return perago.Wrap(perago.StoreFailed, err, "encode batch options")
	}
	ids, _ := json.Marshal(append([]string{}, b.FailedJobIDs...))
	_, err = run(ctx, s.db, perago.StoreFailed, s.sql.insert(batchCols...),
		b.ID, b.Name, b.TotalJobs, b.PendingJobs, b.FailedJobs, string(ids), string(opts),
		nullUnix(b.CancelledAt), b.CreatedAt.Unix(), nullUnix(b.FinishedAt))
	return err
}

func (s *BatchStore) scan(sc interface{ Scan(...any) error }) (*perago.Batch, error) {
	var b perago.Batch
	var ids, opts sql.NullString
	var cancelled, finished sql.NullInt64
	var created int64
	if err := sc.Scan(&b.ID, &b.Name, &b.TotalJobs, &b.PendingJobs, &b.FailedJobs, &ids, &opts, &cancelled, &created, &finished); err != nil {
		return nil, err
	}
	b.CreatedAt = time.Unix(created, 0)
	if cancelled.Valid {
		t := time.Unix(cancelled.Int64, 0)
		b.CancelledAt = &t
	}
	if finished.Valid {
		t := time.Unix(finished.Int64, 0)
		b.FinishedAt = &t
	}
	if ids.Valid && ids.String != "" {
		if err := json.Unmarshal([]byte(ids.String), &b.FailedJobIDs); err != nil {
			return nil, perago.Wrap(perago.StoreFailed, err, "decode failed job ids")
		}
	}
	if opts.Valid && opts.String != "" {
		if err := json.Unmarshal([]byte(opts.String), &b.Options); err != nil {
			return nil, perago.Wrap(perago.StoreFailed, err, "decode batch options")
		}
	}
	return &b, nil
}

func (s *BatchStore) find(ctx context.Context, db *migro.DB, id string) (*perago.Batch, error) {
	rows, err := db.Gorm.WithContext(ctx).Raw("SELECT "+s.sql.cols(batchCols...)+" FROM "+s.sql.t()+" WHERE "+s.sql.q("id")+" = ?", id).Rows()
	if err != nil {
		return nil, perago.Wrap(perago.StoreFailed, err, "find batch")
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, perago.Wrap(perago.StoreFailed, err, "find batch")
		}
		return nil, perago.Newf(perago.BatchNotFound, "batch %s not found", id)
	}
	b, err := s.scan(rows)
	if err != nil {
		return nil, perago.Wrap(perago.StoreFailed, err, "scan batch")
	}
	return b, nil
}

func (s *BatchStore) Find(ctx context.Context, id string) (*perago.Batch, error) {
	return s.find(ctx, s.db, id)
}

func (s *BatchStore) List(ctx context.Context, limit int, before string) ([]*perago.Batch, error) {
	if limit <= 0 {
		limit = 50
	}
	where := ""
	var args []any
	if before != "" {
		b, err := s.Find(ctx, before)
		if err != nil {
			return nil, err
		}
		where = " WHERE " + s.sql.q("created_at") + " < ? OR (" + s.sql.q("created_at") + " = ? AND " + s.sql.q("id") + " < ?)"
		args = append(args, b.CreatedAt.Unix(), b.CreatedAt.Unix(), b.ID)
	}
	q := s.sql.limit(s.sql.cols(batchCols...)+" FROM "+s.sql.t()+where+" ORDER BY "+s.sql.q("created_at")+" DESC, "+s.sql.q("id")+" DESC", limit)
	rows, err := s.db.Gorm.WithContext(ctx).Raw(q, args...).Rows()
	if err != nil {
		return nil, perago.Wrap(perago.StoreFailed, err, "list batches")
	}
	defer rows.Close()
	var out []*perago.Batch
	for rows.Next() {
		b, err := s.scan(rows)
		if err != nil {
			return nil, perago.Wrap(perago.StoreFailed, err, "scan batch")
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *BatchStore) IncrementTotalJobs(ctx context.Context, id string, n int) error {
	q := s.sql.q
	_, err := run(ctx, s.db, perago.StoreFailed, "UPDATE "+s.sql.t()+" SET "+q("total_jobs")+" = "+q("total_jobs")+" + ?, "+
		q("pending_jobs")+" = "+q("pending_jobs")+" + ?, "+q("finished_at")+" = NULL WHERE "+q("id")+" = ?", n, n, id)
	return err
}

// update changes the counters first, so the row stays locked by that UPDATE for the rest of the
// transaction on every dialect, then reads the row and rewrites failed_job_ids.
func (s *BatchStore) update(ctx context.Context, id, counter string, delta int, ids func([]string) []string) (*perago.Batch, error) {
	var out *perago.Batch
	err := s.db.Transaction(ctx, func(tx *migro.DB) error {
		q := s.sql.q
		n, err := run(ctx, tx, perago.StoreFailed, "UPDATE "+s.sql.t()+" SET "+q(counter)+" = "+q(counter)+" + ? WHERE "+q("id")+" = ?", delta, id)
		if err != nil {
			return err
		}
		if n == 0 {
			return perago.Newf(perago.BatchNotFound, "batch %s not found", id)
		}
		b, err := s.find(ctx, tx, id)
		if err != nil {
			return err
		}
		b.FailedJobIDs = ids(b.FailedJobIDs)
		raw, _ := json.Marshal(append([]string{}, b.FailedJobIDs...))
		if _, err := run(ctx, tx, perago.StoreFailed, "UPDATE "+s.sql.t()+" SET "+q("failed_job_ids")+" = ? WHERE "+q("id")+" = ?", string(raw), id); err != nil {
			return err
		}
		out = b
		return nil
	})
	return out, err
}

func (s *BatchStore) RecordSuccess(ctx context.Context, id, jobID string) (*perago.Batch, error) {
	return s.update(ctx, id, "pending_jobs", -1, func(ids []string) []string {
		out := ids[:0:0]
		for _, x := range ids {
			if x != jobID {
				out = append(out, x)
			}
		}
		return out
	})
}

func (s *BatchStore) RecordFailure(ctx context.Context, id, jobID string) (*perago.Batch, error) {
	return s.update(ctx, id, "failed_jobs", 1, func(ids []string) []string { return append(ids, jobID) })
}

func (s *BatchStore) set(ctx context.Context, id string, sets string, args ...any) error {
	_, err := run(ctx, s.db, perago.StoreFailed, "UPDATE "+s.sql.t()+" SET "+sets+" WHERE "+s.sql.q("id")+" = ?", append(args, id)...)
	return err
}

func (s *BatchStore) MarkFinished(ctx context.Context, id string, at time.Time) error {
	return s.set(ctx, id, s.sql.q("finished_at")+" = ?", at.Unix())
}

func (s *BatchStore) Cancel(ctx context.Context, id string, at time.Time) error {
	return s.set(ctx, id, s.sql.q("cancelled_at")+" = ?, "+s.sql.q("finished_at")+" = ?", at.Unix(), at.Unix())
}

func (s *BatchStore) Delete(ctx context.Context, id string) error {
	_, err := run(ctx, s.db, perago.StoreFailed, "DELETE FROM "+s.sql.t()+" WHERE "+s.sql.q("id")+" = ?", id)
	return err
}

func (s *BatchStore) Prune(ctx context.Context, before time.Time) (int64, error) {
	q := s.sql.q
	return run(ctx, s.db, perago.StoreFailed, "DELETE FROM "+s.sql.t()+" WHERE "+q("finished_at")+" IS NOT NULL AND "+q("finished_at")+" < ?", before.Unix())
}

func (s *BatchStore) PruneUnfinished(ctx context.Context, before time.Time) (int64, error) {
	q := s.sql.q
	return run(ctx, s.db, perago.StoreFailed, "DELETE FROM "+s.sql.t()+" WHERE "+q("finished_at")+" IS NULL AND "+q("created_at")+" < ?", before.Unix())
}

func (s *BatchStore) PruneCancelled(ctx context.Context, before time.Time) (int64, error) {
	q := s.sql.q
	return run(ctx, s.db, perago.StoreFailed, "DELETE FROM "+s.sql.t()+" WHERE "+q("cancelled_at")+" IS NOT NULL AND "+q("cancelled_at")+" < ?", before.Unix())
}

// Cache is a perago.Cache in the cache table. Values longer than CacheValueLength are rejected.
type Cache struct {
	db  *migro.DB
	sql sqlBuilder
	now func() time.Time
}

// NewCache returns the database cache ("" uses cache).
func NewCache(db *migro.DB, table string) *Cache {
	return &Cache{db: db, sql: sqlBuilder{db: db, table: or(table, CacheTable)}, now: time.Now}
}

// SetClock replaces the cache's clock (tests).
func (c *Cache) SetClock(now func() time.Time) { c.now = now }

// keyLength is the key column's size; longer keys are stored as their SHA-256.
const keyLength = 255

func (c *Cache) key(k string) string {
	if len(k) <= keyLength {
		return k
	}
	sum := sha256.Sum256([]byte(k))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func (c *Cache) expiration(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	return c.now().Add(ttl).Unix()
}

func (c *Cache) row(ctx context.Context, key string) (value string, exp int64, ok bool, err error) {
	q := c.sql.q
	rows, err := c.db.Gorm.WithContext(ctx).Raw("SELECT "+q("value")+", "+q("expiration")+" FROM "+c.sql.t()+" WHERE "+q("key")+" = ?", key).Rows()
	if err != nil {
		return "", 0, false, perago.Wrap(perago.CacheFailed, err, "get %q", key)
	}
	defer rows.Close()
	if !rows.Next() {
		return "", 0, false, rows.Err()
	}
	if err := rows.Scan(&value, &exp); err != nil {
		return "", 0, false, perago.Wrap(perago.CacheFailed, err, "scan %q", key)
	}
	if exp > 0 && exp <= c.now().Unix() {
		return value, exp, false, nil
	}
	return value, exp, true, nil
}

func (c *Cache) check(value string) error {
	if len(value) > CacheValueLength {
		return perago.Newf(perago.CacheFailed, "value of %d bytes is longer than the database cache's %d", len(value), CacheValueLength)
	}
	return nil
}

func (c *Cache) Get(ctx context.Context, key string) (string, bool, error) {
	key = c.key(key)
	v, _, ok, err := c.row(ctx, key)
	if !ok {
		return "", false, err
	}
	return v, true, err
}

func (c *Cache) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	key = c.key(key)
	if err := c.check(value); err != nil {
		return err
	}
	q := c.sql.q
	exp := c.expiration(ttl)
	for range 3 {
		n, err := run(ctx, c.db, perago.CacheFailed, "UPDATE "+c.sql.t()+" SET "+q("value")+" = ?, "+q("expiration")+" = ? WHERE "+q("key")+" = ?", value, exp, key)
		if err != nil || n > 0 {
			return err
		}
		if _, err := run(ctx, c.db, perago.CacheFailed, c.sql.insert("key", "value", "expiration"), key, value, exp); err == nil {
			return nil
		}
	}
	return perago.Newf(perago.CacheFailed, "put %q: concurrent writers kept racing", key)
}

func (c *Cache) Add(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	key = c.key(key)
	if err := c.check(value); err != nil {
		return false, err
	}
	q := c.sql.q
	exp := c.expiration(ttl)
	_, insertErr := run(ctx, c.db, perago.CacheFailed, c.sql.insert("key", "value", "expiration"), key, value, exp)
	if insertErr == nil {
		return true, nil
	}
	now := c.now().Unix()
	n, err := run(ctx, c.db, perago.CacheFailed, "UPDATE "+c.sql.t()+" SET "+q("value")+" = ?, "+q("expiration")+" = ? WHERE "+
		q("key")+" = ? AND "+q("expiration")+" > 0 AND "+q("expiration")+" <= ?", value, exp, key, now)
	if err != nil {
		return false, err
	}
	if n == 1 {
		return true, nil
	}
	exists, err := c.exists(ctx, key)
	if err != nil || exists {
		return false, err
	}
	return false, insertErr
}

// exists tells a duplicate-key insert error (the key is held) from a real failure.
func (c *Cache) exists(ctx context.Context, key string) (bool, error) {
	var n int64
	row := c.db.Gorm.WithContext(ctx).Raw("SELECT COUNT(*) FROM "+c.sql.t()+" WHERE "+c.sql.q("key")+" = ?", key).Row()
	if err := row.Scan(&n); err != nil {
		return false, perago.Wrap(perago.CacheFailed, err, "exists %q", key)
	}
	return n > 0, nil
}

// Increment uses compare-and-swap on the stored value, portable across dialects.
func (c *Cache) Increment(ctx context.Context, key string, by int64, ttl time.Duration) (int64, error) {
	key = c.key(key)
	q := c.sql.q
	for range 100 {
		v, _, ok, err := c.row(ctx, key)
		if err != nil {
			return 0, err
		}
		if !ok {
			added, err := c.Add(ctx, key, strconv.FormatInt(by, 10), ttl)
			if err != nil {
				return 0, err
			}
			if added {
				return by, nil
			}
			continue
		}
		cur, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, perago.Wrap(perago.CacheFailed, err, "increment %q: not an integer", key)
		}
		next := strconv.FormatInt(cur+by, 10)
		n, err := run(ctx, c.db, perago.CacheFailed, "UPDATE "+c.sql.t()+" SET "+q("value")+" = ? WHERE "+q("key")+" = ? AND "+q("value")+" = ?", next, key, v)
		if err != nil {
			return 0, err
		}
		if n == 1 {
			return cur + by, nil
		}
	}
	return 0, perago.Newf(perago.CacheFailed, "increment %q: too much contention", key)
}

func (c *Cache) Forget(ctx context.Context, key string) error {
	key = c.key(key)
	_, err := run(ctx, c.db, perago.CacheFailed, "DELETE FROM "+c.sql.t()+" WHERE "+c.sql.q("key")+" = ?", key)
	return err
}

func (c *Cache) ForgetIf(ctx context.Context, key, value string) (bool, error) {
	key = c.key(key)
	q := c.sql.q
	n, err := run(ctx, c.db, perago.CacheFailed, "DELETE FROM "+c.sql.t()+" WHERE "+q("key")+" = ? AND "+q("value")+" = ?", key, value)
	return n > 0, err
}

// Transaction runs fn in a database transaction and an after-commit scope of m: jobs dispatched
// with the context fn receives to an AfterCommit connection are pushed only once the transaction
// commits, and dropped when it rolls back.
func Transaction(ctx context.Context, m *perago.Manager, db *migro.DB, fn func(ctx context.Context, tx *migro.DB) error) error {
	return m.Transaction(ctx, func(ctx context.Context) error {
		return db.Transaction(ctx, func(tx *migro.DB) error { return fn(ctx, tx) })
	})
}

package perago

import (
	"context"
	"sort"
	"sync"
	"time"
)

// FailedJob is a row of failed_jobs.
type FailedJob struct {
	ID         string // the job UUID
	Connection string
	Queue      string
	Payload    []byte
	Exception  string
	FailedAt   time.Time
}

// FailedJobStore persists failed jobs (Laravel's FailedJobProvider).
type FailedJobStore interface {
	Log(ctx context.Context, f FailedJob) error
	// All returns every failed job, newest first.
	All(ctx context.Context) ([]FailedJob, error)
	// Find returns ErrFailedJobNotFound when the id is unknown.
	Find(ctx context.Context, id string) (*FailedJob, error)
	// IDs lists the failed job ids, of queue only when queue is not empty.
	IDs(ctx context.Context, queue string) ([]string, error)
	Forget(ctx context.Context, id string) (bool, error)
	// Flush deletes every failed job, or only those older than olderThan when it is positive.
	Flush(ctx context.Context, olderThan time.Duration) (int64, error)
	// Prune deletes the failed jobs that failed before t.
	Prune(ctx context.Context, before time.Time) (int64, error)
}

// NullFailedJobStore stores nothing (failed job storage disabled).
type NullFailedJobStore struct{}

func (NullFailedJobStore) Log(context.Context, FailedJob) error          { return nil }
func (NullFailedJobStore) All(context.Context) ([]FailedJob, error)      { return nil, nil }
func (NullFailedJobStore) IDs(context.Context, string) ([]string, error) { return nil, nil }
func (NullFailedJobStore) Find(_ context.Context, id string) (*FailedJob, error) {
	return nil, Newf(FailedJobNotFound, "failed job %s not found", id)
}
func (NullFailedJobStore) Forget(context.Context, string) (bool, error)        { return false, nil }
func (NullFailedJobStore) Flush(context.Context, time.Duration) (int64, error) { return 0, nil }
func (NullFailedJobStore) Prune(context.Context, time.Time) (int64, error)     { return 0, nil }

// MemoryFailedJobStore keeps failed jobs in memory.
type MemoryFailedJobStore struct {
	mu   sync.Mutex
	jobs []FailedJob
	now  func() time.Time
}

// NewMemoryFailedJobStore returns an empty in-memory failed job store.
func NewMemoryFailedJobStore() *MemoryFailedJobStore { return &MemoryFailedJobStore{now: time.Now} }

func (s *MemoryFailedJobStore) Log(_ context.Context, f FailedJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, j := range s.jobs {
		if j.ID == f.ID {
			s.jobs[i] = f
			return nil
		}
	}
	s.jobs = append(s.jobs, f)
	return nil
}

func (s *MemoryFailedJobStore) All(context.Context) ([]FailedJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]FailedJob(nil), s.jobs...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].FailedAt.After(out[j].FailedAt) })
	return out, nil
}

func (s *MemoryFailedJobStore) IDs(_ context.Context, queue string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, j := range s.jobs {
		if queue == "" || j.Queue == queue {
			out = append(out, j.ID)
		}
	}
	return out, nil
}

func (s *MemoryFailedJobStore) Find(_ context.Context, id string) (*FailedJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.jobs {
		if j.ID == id {
			c := j
			return &c, nil
		}
	}
	return nil, Newf(FailedJobNotFound, "failed job %s not found", id)
}

func (s *MemoryFailedJobStore) Forget(_ context.Context, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, j := range s.jobs {
		if j.ID == id {
			s.jobs = append(s.jobs[:i], s.jobs[i+1:]...)
			return true, nil
		}
	}
	return false, nil
}

func (s *MemoryFailedJobStore) remove(match func(FailedJob) bool) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.jobs[:0]
	var n int64
	for _, j := range s.jobs {
		if match(j) {
			n++
			continue
		}
		kept = append(kept, j)
	}
	s.jobs = kept
	return n
}

func (s *MemoryFailedJobStore) Flush(_ context.Context, olderThan time.Duration) (int64, error) {
	if olderThan <= 0 {
		return s.remove(func(FailedJob) bool { return true }), nil
	}
	cut := s.now().Add(-olderThan)
	return s.remove(func(j FailedJob) bool { return j.FailedAt.Before(cut) }), nil
}

func (s *MemoryFailedJobStore) Prune(_ context.Context, before time.Time) (int64, error) {
	return s.remove(func(j FailedJob) bool { return j.FailedAt.Before(before) }), nil
}

// RetryFailed pushes failed jobs back onto their original connection and queue with fresh
// attempts, then forgets them (queue:retry).
func (m *Manager) RetryFailed(ctx context.Context, ids ...string) error {
	for _, id := range ids {
		f, err := m.failed.Find(ctx, id)
		if err != nil {
			return err
		}
		m.events.emit(ctx, JobRetryRequested{Failed: *f})
		p, err := DecodePayload(f.Payload)
		if err != nil {
			return err
		}
		m.refreshRetryUntil(ctx, p)
		conn := f.Connection
		if _, ok := m.conns[conn]; !ok {
			return Newf(ConnectionNotFound, "failed job %s belongs to unknown connection %q", id, conn)
		}
		p.Queue = f.Queue
		if _, err := m.pushPayload(ctx, conn, p, 0); err != nil {
			return err
		}
		if _, err := m.failed.Forget(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

// RetryFailedQueue retries every failed job of queue (queue:retry --queue).
func (m *Manager) RetryFailedQueue(ctx context.Context, queue string) error {
	ids, err := m.failed.IDs(ctx, queue)
	if err != nil {
		return err
	}
	return m.RetryFailed(ctx, ids...)
}

// RetryBatch retries the failed jobs of a batch (queue:retry-batch).
func (m *Manager) RetryBatch(ctx context.Context, id string) error {
	b, err := m.FindBatch(ctx, id)
	if err != nil {
		return err
	}
	return m.RetryFailed(ctx, b.FailedJobIDs...)
}

func (m *Manager) refreshRetryUntil(ctx context.Context, p *Payload) {
	if p.RetryUntil == 0 {
		return
	}
	job, err := m.decodeJob(ctx, p, false)
	if err != nil {
		return
	}
	p.RetryUntil = 0
	if r, ok := job.(withRetryFor); ok {
		p.RetryUntil = m.now().Add(r.RetryFor()).Unix()
	}
	if r, ok := job.(withRetryUntil); ok && !r.RetryUntil().IsZero() {
		p.RetryUntil = r.RetryUntil().Unix()
	}
}

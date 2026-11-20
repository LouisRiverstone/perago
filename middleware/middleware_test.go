package middleware_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/middleware"
)

var ctx = context.Background()

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type env struct {
	m     *perago.Manager
	q     *perago.MemoryDriver
	clock *clock
	cache *perago.MemoryCache
}

func newEnv(t *testing.T) *env {
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	q := perago.NewMemoryDriver(0)
	q.SetClock(c.Now)
	cache := perago.NewMemoryCache()
	cache.SetClock(c.Now)
	m, err := perago.New(perago.Config{Default: "mem", Connections: map[string]perago.Connection{"mem": {Driver: q}}, Cache: cache, Now: c.Now, Failed: perago.NewMemoryFailedJobStore(), Batches: perago.NewMemoryBatchStore()})
	if err != nil {
		t.Fatal(err)
	}
	return &env{m: m, q: q, clock: c, cache: cache}
}

func (e *env) once(t *testing.T) {
	t.Helper()
	if _, err := e.m.Work(ctx, perago.WorkerOptions{Once: true, Tries: -1}); err != nil {
		t.Fatal(err)
	}
}

func (e *env) size() int64 { n, _ := e.q.Size(ctx, "default"); return n }

var ran atomic.Int32

type Limited struct{ User string }

func (j *Limited) Handle(context.Context) error { ran.Add(1); return nil }
func (j *Limited) Middleware() []perago.Middleware {
	return []perago.Middleware{middleware.RateLimited("per-user")}
}

func init() { perago.Register(&Limited{}) }

func TestRateLimited(t *testing.T) {
	e := newEnv(t)
	ran.Store(0)
	e.m.RateLimiter("per-user", func(job perago.Job) []perago.Limit {
		return []perago.Limit{perago.PerMinute(2).By(job.(*Limited).User)}
	})
	var released []time.Duration
	perago.Listen(e.m, func(ctx context.Context, ev perago.JobProcessed) {})
	for range 3 {
		_, _ = e.m.Dispatch(&Limited{User: "u1"}).Send(ctx)
	}
	_, _ = e.m.Dispatch(&Limited{User: "u2"}).Send(ctx)
	for range 4 {
		e.once(t)
	}
	if ran.Load() != 3 || e.size() != 1 {
		t.Fatalf("ran=%d left=%d released=%v", ran.Load(), e.size(), released)
	}
	e.clock.Advance(2 * time.Minute)
	e.once(t)
	if ran.Load() != 4 {
		t.Fatalf("after window ran=%d", ran.Load())
	}
}

type Exclusive struct {
	perago.Queueable
	Key string
}

var exclusiveInner func()

func (j *Exclusive) Handle(context.Context) error {
	ran.Add(1)
	if exclusiveInner != nil {
		exclusiveInner()
	}
	return nil
}
func (j *Exclusive) Middleware() []perago.Middleware {
	return []perago.Middleware{middleware.WithoutOverlapping(j.Key).ReleaseAfter(10 * time.Second).ExpireAfter(time.Minute)}
}

func init() { perago.Register(&Exclusive{}) }

func TestWithoutOverlapping(t *testing.T) {
	e := newEnv(t)
	ran.Store(0)
	_, _ = e.m.Dispatch(&Exclusive{Key: "k"}).Send(ctx)
	_, _ = e.m.Dispatch(&Exclusive{Key: "k"}).Send(ctx)
	// While the first job runs, the second one finds the lock held.
	exclusiveInner = func() {
		exclusiveInner = nil
		_, _ = e.m.Work(ctx, perago.WorkerOptions{Once: true})
	}
	e.once(t)
	if ran.Load() != 1 || e.size() != 1 {
		t.Fatalf("ran=%d left=%d", ran.Load(), e.size())
	}
	e.clock.Advance(11 * time.Second)
	e.once(t)
	if ran.Load() != 2 || e.size() != 0 {
		t.Fatalf("after lock released ran=%d left=%d", ran.Load(), e.size())
	}
}

var errUpstream = errors.New("upstream down")
var errGone = errors.New("gone")

type Throttled struct{ Gone bool }

func (j *Throttled) Handle(context.Context) error {
	ran.Add(1)
	if j.Gone {
		return errGone
	}
	return errUpstream
}
func (j *Throttled) Middleware() []perago.Middleware {
	return []perago.Middleware{middleware.ThrottlesExceptions(2, 5*time.Minute).Backoff(time.Second).DeleteWhen(errGone)}
}

func init() { perago.Register(&Throttled{}) }

func TestThrottlesExceptions(t *testing.T) {
	e := newEnv(t)
	ran.Store(0)
	_, _ = e.m.Dispatch(&Throttled{}).Send(ctx)
	e.once(t)
	e.clock.Advance(2 * time.Second)
	e.once(t)
	e.clock.Advance(2 * time.Second)
	e.once(t) // throttled: released without running
	if ran.Load() != 2 {
		t.Fatalf("ran=%d", ran.Load())
	}
	e.clock.Advance(6 * time.Minute)
	e.once(t)
	if ran.Load() != 3 {
		t.Fatalf("after decay ran=%d", ran.Load())
	}
	_, _ = e.q.Clear(ctx, "default")
	_, _ = e.m.Dispatch(&Throttled{Gone: true}).Send(ctx)
	e.clock.Advance(time.Hour)
	_ = e.cache.Forget(ctx, "x")
	_, _ = e.m.Work(ctx, perago.WorkerOptions{StopWhenEmpty: true})
	if e.size() != 0 {
		t.Fatal("DeleteWhen did not delete")
	}
}

type Conditional struct {
	Skip, Release bool
}

func (j *Conditional) Handle(context.Context) error { ran.Add(1); return nil }
func (j *Conditional) Middleware() []perago.Middleware {
	return []perago.Middleware{
		middleware.SkipWhen(middleware.Always(j.Skip)),
		middleware.ReleaseWhen(func(ctx context.Context, job perago.Job) bool { return job.(*Conditional).Release }, time.Minute),
	}
}

func init() { perago.Register(&Conditional{}) }

func TestSkipAndRelease(t *testing.T) {
	e := newEnv(t)
	ran.Store(0)
	_, _ = e.m.Dispatch(&Conditional{Skip: true}).Send(ctx)
	e.once(t)
	if ran.Load() != 0 || e.size() != 0 {
		t.Fatal("skip")
	}
	_, _ = e.m.Dispatch(&Conditional{Release: true}).Send(ctx)
	e.once(t)
	if ran.Load() != 0 || e.size() != 1 {
		t.Fatal("release")
	}
	_, _ = e.q.Clear(ctx, "default")
	_, _ = e.m.Dispatch(&Conditional{}).Send(ctx)
	e.once(t)
	if ran.Load() != 1 {
		t.Fatal("pass-through")
	}
}

type InBatch struct{ perago.Queueable }

func (j *InBatch) Handle(ctx context.Context) error {
	ran.Add(1)
	b, _ := j.Batch(ctx)
	return b.Cancel(ctx)
}
func (j *InBatch) Middleware() []perago.Middleware {
	return []perago.Middleware{middleware.SkipIfBatchCancelled()}
}

func init() { perago.Register(&InBatch{}) }

func TestSkipIfBatchCancelled(t *testing.T) {
	e := newEnv(t)
	ran.Store(0)
	if _, err := e.m.Batch(&InBatch{}, &InBatch{}).Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	_, _ = e.m.Work(ctx, perago.WorkerOptions{StopWhenEmpty: true})
	if ran.Load() != 1 {
		t.Fatalf("ran=%d", ran.Load())
	}
}

var errForbidden = errors.New("forbidden")

type Authz struct{}

func (Authz) Handle(context.Context) error { ran.Add(1); return errForbidden }
func (Authz) Tries() int                   { return 5 }
func (Authz) Middleware() []perago.Middleware {
	return []perago.Middleware{middleware.FailOnException(errForbidden)}
}

func init() { perago.Register(&Authz{}) }

func TestFailOnException(t *testing.T) {
	e := newEnv(t)
	ran.Store(0)
	_, _ = e.m.Dispatch(&Authz{}).Send(ctx)
	_, _ = e.m.Work(ctx, perago.WorkerOptions{StopWhenEmpty: true})
	if ran.Load() != 1 || e.size() != 0 {
		t.Fatalf("ran=%d left=%d", ran.Load(), e.size())
	}
	all, _ := e.m.FailedJobs().All(ctx)
	if len(all) != 1 {
		t.Fatalf("failed = %v", all)
	}
}

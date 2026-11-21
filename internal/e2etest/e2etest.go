// Package e2etest is perago's end-to-end suite: Laravel's queue scenarios run through the
// Manager and a real worker on top of one backend (a driver, a cache, a failed job store and a
// batch store). Every backend runs it: memory in the core module, the database driver on every
// dialect in internal/dbtest and migro, Redis in its module.
//
// The suite controls time through a fake clock handed to the backend, so delays, backoff, rate
// limit windows and lock expiry are tested without sleeping.
package e2etest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/middleware"
)

// Backend is what a factory builds for one test, on the clock it is given.
type Backend struct {
	Driver  perago.Driver
	Cache   perago.Cache          // nil: an in-process MemoryCache
	Failed  perago.FailedJobStore // required
	Batches perago.BatchStore     // required
	// Advance moves backend time that the clock does not drive (miniredis TTLs). Optional.
	Advance func(time.Duration)
}

// Factory returns a fresh, empty backend whose driver, cache and stores read now.
type Factory func(t *testing.T, now func() time.Time) Backend

const conn = "e2e"

var key = []byte("0123456789abcdef0123456789abcdef")

type env struct {
	t     *testing.T
	m     *perago.Manager
	b     Backend
	run   string
	clock atomic.Int64 // unix nanoseconds
}

func newEnv(t *testing.T, factory Factory) *env {
	t.Helper()
	e := &env{t: t, run: perago.NewUUID()}
	e.clock.Store(time.Unix(1_800_000_000, 0).UnixNano())
	e.b = factory(t, e.now)
	cache := e.b.Cache
	if cache == nil {
		mc := perago.NewMemoryCache()
		mc.SetClock(e.now)
		cache = mc
	}
	m, err := perago.New(perago.Config{
		Default:     conn,
		Connections: map[string]perago.Connection{conn: {Driver: e.b.Driver}},
		Failed:      e.b.Failed,
		Batches:     e.b.Batches,
		Cache:       cache,
		Key:         key,
		Now:         e.now,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close(); forget(e.run) })
	e.m = m
	return e
}

func (e *env) now() time.Time { return time.Unix(0, e.clock.Load()) }

func (e *env) advance(d time.Duration) {
	e.clock.Add(int64(d))
	if e.b.Advance != nil {
		e.b.Advance(d)
	}
}

func (e *env) size(queue string) int64 {
	e.t.Helper()
	n, err := e.m.Size(context.Background(), conn, queue)
	if err != nil {
		e.t.Fatal(err)
	}
	return n
}

func (e *env) work(o perago.WorkerOptions) {
	e.t.Helper()
	o.StopWhenEmpty = true
	o.Sleep = time.Millisecond
	if _, err := e.m.Work(context.Background(), o); err != nil {
		e.t.Fatal(err)
	}
}

// drain works the queues until they are empty, moving the clock past delays and backoff.
func (e *env) drain(o perago.WorkerOptions, queues ...string) {
	e.t.Helper()
	if len(queues) == 0 {
		queues = []string{"default"}
	}
	o.Queues = queues
	for range 30 {
		e.work(o)
		left := int64(0)
		for _, q := range queues {
			left += e.size(q)
		}
		if left == 0 {
			return
		}
		e.advance(time.Hour)
	}
	e.t.Fatal("queues never drained")
}

func (e *env) runs() []string { return take(e.run) }

// failures describes the failed jobs, for error messages.
func (e *env) failures() string {
	all, err := e.b.Failed.All(context.Background())
	if err != nil {
		return err.Error()
	}
	var out []string
	for _, f := range all {
		out = append(out, f.Queue+": "+f.Exception)
	}
	return "failed jobs: [" + strings.Join(out, "; ") + "]"
}

// Run runs every scenario against the backend.
func Run(t *testing.T, factory Factory) {
	ctx := context.Background()

	t.Run("payload round trip", func(t *testing.T) {
		e := newEnv(t, factory)
		job := &Echo{Run: e.run, Msg: "olá, ✓", N: 42, Tags: []string{"a", "b"}, Meta: map[string]int{"x": 1}, At: e.now().UTC()}
		if id, err := e.m.Dispatch(job).Send(ctx); err != nil || id == "" {
			t.Fatalf("dispatch: %q %v", id, err)
		}
		if n := e.size("default"); n != 1 {
			t.Fatalf("size = %d", n)
		}
		e.drain(perago.WorkerOptions{})
		want := fmt.Sprintf("echo olá, ✓ 42 [a b] map[x:1] %d", e.now().Unix())
		if got := e.runs(); len(got) != 1 || got[0] != want {
			t.Fatalf("runs = %q, want %q", got, want)
		}
	})

	t.Run("delayed job waits", func(t *testing.T) {
		e := newEnv(t, factory)
		_, _ = e.m.Dispatch(&Echo{Run: e.run, Msg: "later"}).Delay(10 * time.Minute).Send(ctx)
		e.work(perago.WorkerOptions{})
		if got := e.runs(); len(got) != 0 {
			t.Fatalf("ran before its delay: %v", got)
		}
		e.advance(11 * time.Minute)
		e.work(perago.WorkerOptions{})
		if got := e.runs(); len(got) != 1 {
			t.Fatalf("runs after delay = %v", got)
		}
	})

	t.Run("retries with backoff", func(t *testing.T) {
		e := newEnv(t, factory)
		var delays []time.Duration
		var mu sync.Mutex
		perago.Listen(e.m, func(ctx context.Context, ev perago.JobReleasedAfterException) {
			mu.Lock()
			delays = append(delays, ev.Delay)
			mu.Unlock()
		})
		_, _ = e.m.Dispatch(&Flaky{Run: e.run, Name: "f", FailTimes: 2, TriesN: 3}).Send(ctx)
		e.work(perago.WorkerOptions{})
		e.advance(5 * time.Second)
		e.work(perago.WorkerOptions{})
		if got := e.runs(); len(got) != 0 || e.size("default") != 1 {
			t.Fatalf("retried before the backoff: %v", got)
		}
		e.drain(perago.WorkerOptions{})
		if got := e.runs(); len(got) != 1 || got[0] != "flaky f attempt 3" {
			t.Fatalf("runs = %v", got)
		}
		if len(delays) != 2 || delays[0] != 10*time.Second || delays[1] != 30*time.Second {
			t.Fatalf("backoff = %v", delays)
		}
		if all, _ := e.b.Failed.All(ctx); len(all) != 0 {
			t.Fatalf("failed jobs = %v", all)
		}
	})

	t.Run("failed job stored and retried", func(t *testing.T) {
		e := newEnv(t, factory)
		_, _ = e.m.Dispatch(&Flaky{Run: e.run, Name: "g", FailTimes: 1, TriesN: 1}).OnQueue("mail").Send(ctx)
		e.drain(perago.WorkerOptions{}, "mail")
		all, err := e.b.Failed.All(ctx)
		if err != nil || len(all) != 1 || all[0].Queue != "mail" || all[0].Connection != conn || !strings.Contains(all[0].Exception, "boom g") {
			t.Fatalf("failed = %+v %v", all, err)
		}
		if failedHooks(e.run) != 1 {
			t.Fatalf("Failed() ran %d times", failedHooks(e.run))
		}
		if err := e.m.RetryFailed(ctx, all[0].ID); err != nil {
			t.Fatal(err)
		}
		e.drain(perago.WorkerOptions{}, "mail")
		if got := e.runs(); len(got) != 1 || got[0] != "flaky g attempt 1" {
			t.Fatalf("retried runs = %v", got)
		}
		if left, _ := e.b.Failed.All(ctx); len(left) != 0 {
			t.Fatalf("retried job still failed: %v", left)
		}
	})

	t.Run("manual release", func(t *testing.T) {
		e := newEnv(t, factory)
		_, _ = e.m.Dispatch(&Releaser{Run: e.run, Times: 2}).Send(ctx)
		e.work(perago.WorkerOptions{})
		if e.size("default") != 1 {
			t.Fatal("released job not back on the queue")
		}
		e.drain(perago.WorkerOptions{})
		if got := e.runs(); len(got) != 1 || got[0] != "released 3" {
			t.Fatalf("runs = %v", got)
		}
	})

	t.Run("timeout fails the job", func(t *testing.T) {
		e := newEnv(t, factory)
		_, _ = e.m.Dispatch(&Slow{Run: e.run}).Send(ctx)
		e.drain(perago.WorkerOptions{Grace: time.Second})
		all, _ := e.b.Failed.All(ctx)
		if len(all) != 1 || !strings.Contains(all[0].Exception, "timed out") {
			t.Fatalf("failed = %+v", all)
		}
	})

	t.Run("chain runs in order and stops on failure", func(t *testing.T) {
		e := newEnv(t, factory)
		if _, err := e.m.Chain(&Echo{Run: e.run, Msg: "1"}, &Echo{Run: e.run, Msg: "2"}, &Echo{Run: e.run, Msg: "3"}).Dispatch(ctx); err != nil {
			t.Fatal(err)
		}
		e.drain(perago.WorkerOptions{})
		if got := strings.Join(e.runs(), ","); got != "echo 1,echo 2,echo 3" {
			t.Fatalf("chain order = %s", got)
		}
		var caught atomic.Int32
		_, _ = e.m.Chain(&Echo{Run: e.run, Msg: "a"}, &Flaky{Run: e.run, Name: "c", FailTimes: 9, TriesN: 1}, &Echo{Run: e.run, Msg: "never"}).
			Catch(func(ctx context.Context, err error) { caught.Add(1) }).Dispatch(ctx)
		e.drain(perago.WorkerOptions{})
		if got := strings.Join(e.runs(), ","); got != "echo a" || caught.Load() != 1 {
			t.Fatalf("failed chain ran %s, catch=%d", got, caught.Load())
		}
	})

	t.Run("batch callbacks and progress", func(t *testing.T) {
		e := newEnv(t, factory)
		var then, finally, progress atomic.Int32
		b, err := e.m.Batch(&Echo{Run: e.run, Msg: "x"}, &Echo{Run: e.run, Msg: "y"}, &Echo{Run: e.run, Msg: "z"}).Name("import").
			Progress(func(ctx context.Context, b *perago.Batch) { progress.Add(1) }).
			Then(func(ctx context.Context, b *perago.Batch) { then.Add(1) }).
			Finally(func(ctx context.Context, b *perago.Batch) { finally.Add(1) }).
			Dispatch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		e.drain(perago.WorkerOptions{})
		fresh, err := e.m.FindBatch(ctx, b.ID)
		if err != nil || !fresh.Finished() || fresh.TotalJobs != 3 || fresh.PendingJobs != 0 || fresh.Progress() != 100 || fresh.Name != "import" {
			t.Fatalf("batch = %+v %v", fresh, err)
		}
		if len(e.runs()) != 3 || then.Load() != 1 || finally.Load() != 1 || progress.Load() != 3 {
			t.Fatalf("then=%d finally=%d progress=%d", then.Load(), finally.Load(), progress.Load())
		}
	})

	t.Run("batch failure cancels, retry-batch finishes it", func(t *testing.T) {
		e := newEnv(t, factory)
		var catch, then atomic.Int32
		b, _ := e.m.Batch(&Flaky{Run: e.run, Name: "b", FailTimes: 1, TriesN: 1}, &Echo{Run: e.run, Msg: "ok"}).
			Catch(func(ctx context.Context, b *perago.Batch, err error) { catch.Add(1) }).
			Then(func(ctx context.Context, b *perago.Batch) { then.Add(1) }).
			Dispatch(ctx)
		e.drain(perago.WorkerOptions{})
		fresh, _ := e.m.FindBatch(ctx, b.ID)
		if !fresh.Cancelled() || fresh.FailedJobs != 1 || len(fresh.FailedJobIDs) != 1 || catch.Load() != 1 || then.Load() != 0 {
			t.Fatalf("batch = %+v catch=%d then=%d", fresh, catch.Load(), then.Load())
		}
		e.runs()

		allowed, _ := e.m.Batch(&Flaky{Run: e.run, Name: "a", FailTimes: 1, TriesN: 1}).AllowFailures().
			Then(func(ctx context.Context, b *perago.Batch) { then.Add(1) }).Dispatch(ctx)
		e.drain(perago.WorkerOptions{})
		if mid, _ := e.m.FindBatch(ctx, allowed.ID); mid.Cancelled() || mid.FailedJobs != 1 {
			t.Fatalf("allowFailures batch = %+v", mid)
		}
		if err := e.m.RetryBatch(ctx, allowed.ID); err != nil {
			t.Fatal(err)
		}
		e.drain(perago.WorkerOptions{})
		done, _ := e.m.FindBatch(ctx, allowed.ID)
		if !done.Finished() || len(done.FailedJobIDs) != 0 || then.Load() != 1 {
			t.Fatalf("retried batch = %+v then=%d", done, then.Load())
		}
	})

	t.Run("unique jobs", func(t *testing.T) {
		e := newEnv(t, factory)
		first, _ := e.m.Dispatch(&UniqueJob{Run: e.run, Key: "a"}).Send(ctx)
		second, _ := e.m.Dispatch(&UniqueJob{Run: e.run, Key: "a"}).Send(ctx)
		other, _ := e.m.Dispatch(&UniqueJob{Run: e.run, Key: "b"}).Send(ctx)
		if first == "" || second != "" || other == "" {
			t.Fatalf("ids %q %q %q", first, second, other)
		}
		e.drain(perago.WorkerOptions{})
		if got := e.runs(); len(got) != 2 {
			t.Fatalf("runs = %v", got)
		}
		if again, _ := e.m.Dispatch(&UniqueJob{Run: e.run, Key: "a"}).Send(ctx); again == "" {
			t.Fatal("unique lock not released after the job ran")
		}
	})

	t.Run("rate limited", func(t *testing.T) {
		e := newEnv(t, factory)
		e.m.RateLimiter(limiterName, func(job perago.Job) []perago.Limit {
			return []perago.Limit{perago.PerMinute(2).By(job.(*Limited).User)}
		})
		for range 3 {
			_, _ = e.m.Dispatch(&Limited{Run: e.run, User: "u1"}).Send(ctx)
		}
		_, _ = e.m.Dispatch(&Limited{Run: e.run, User: "u2"}).Send(ctx)
		for range 4 {
			e.work(perago.WorkerOptions{Once: true})
		}
		if got := e.runs(); len(got) != 3 || e.size("default") != 1 {
			t.Fatalf("ran %v, left %d", got, e.size("default"))
		}
		e.advance(2 * time.Minute)
		e.work(perago.WorkerOptions{})
		if got := e.runs(); len(got) != 1 || got[0] != "limited u1" {
			t.Fatalf("after the window = %v, left %d, %s", got, e.size("default"), e.failures())
		}
	})

	t.Run("without overlapping", func(t *testing.T) {
		e := newEnv(t, factory)
		_, _ = e.m.Dispatch(&Exclusive{Run: e.run, Key: "k"}).Send(ctx)
		_, _ = e.m.Dispatch(&Exclusive{Run: e.run, Key: "k"}).Send(ctx)
		// The first job starts a second worker while it holds the lock.
		setInner(e.run, func() { e.work(perago.WorkerOptions{Once: true}) })
		e.work(perago.WorkerOptions{Once: true})
		if got := e.runs(); len(got) != 1 || e.size("default") != 1 {
			t.Fatalf("overlapping job ran: %v, left %d", got, e.size("default"))
		}
		e.advance(11 * time.Second)
		e.work(perago.WorkerOptions{})
		if got := e.runs(); len(got) != 1 || e.size("default") != 0 {
			t.Fatalf("after the lock = %v, left %d, %s", got, e.size("default"), e.failures())
		}
	})

	t.Run("encrypted payload", func(t *testing.T) {
		e := newEnv(t, factory)
		_, _ = e.m.Dispatch(&Secret{Run: e.run, Card: "4111-1111"}).Send(ctx)
		r, err := e.b.Driver.Pop(ctx, "default")
		if err != nil || r == nil {
			t.Fatalf("pop: %v %v", r, err)
		}
		if raw := string(r.Payload()); strings.Contains(raw, "4111") {
			t.Fatalf("stored in clear: %s", raw)
		}
		if err := r.Release(ctx, 0); err != nil {
			t.Fatal(err)
		}
		e.drain(perago.WorkerOptions{})
		if got := e.runs(); len(got) != 1 || got[0] != "card 4111-1111" {
			t.Fatalf("runs = %v, %s", got, e.failures())
		}
	})

	t.Run("pause, resume and restart", func(t *testing.T) {
		e := newEnv(t, factory)
		_, _ = e.m.Dispatch(&Echo{Run: e.run, Msg: "p"}).Send(ctx)
		if err := e.m.Pause(ctx, conn, "default", 0); err != nil {
			t.Fatal(err)
		}
		if paused, _ := e.m.IsPaused(ctx, conn, "default"); !paused {
			t.Fatal("not paused")
		}
		e.work(perago.WorkerOptions{})
		if got := e.runs(); len(got) != 0 {
			t.Fatalf("paused queue ran %v", got)
		}
		if err := e.m.Resume(ctx, conn, "default"); err != nil {
			t.Fatal(err)
		}
		e.work(perago.WorkerOptions{})
		if got := e.runs(); len(got) != 1 {
			t.Fatalf("after resume = %v", got)
		}

		// A restart signalled while a worker runs stops it after the current job.
		for _, m := range []string{"r1", "r2"} {
			_, _ = e.m.Dispatch(&Restarter{Run: e.run, Msg: m}).Send(ctx)
		}
		setInner(e.run, func() {
			e.advance(time.Second)
			_ = e.m.Restart(context.Background())
		})
		code, err := e.m.Work(ctx, perago.WorkerOptions{Sleep: time.Millisecond})
		if err != nil || code != perago.ExitSuccess || e.size("default") != 1 {
			t.Fatalf("restart: code=%d err=%v left=%d", code, err, e.size("default"))
		}
		e.runs()
	})

	t.Run("queue priorities", func(t *testing.T) {
		e := newEnv(t, factory)
		_, _ = e.m.Dispatch(&Echo{Run: e.run, Msg: "low"}).OnQueue("low").Send(ctx)
		_, _ = e.m.Dispatch(&Echo{Run: e.run, Msg: "high"}).OnQueue("high").Send(ctx)
		e.drain(perago.WorkerOptions{}, "high", "low")
		if got := strings.Join(e.runs(), ","); got != "echo high,echo low" {
			t.Fatalf("order = %s", got)
		}
	})

	t.Run("concurrent workers run each job once", func(t *testing.T) {
		e := newEnv(t, factory)
		const jobs, workers = 40, 4
		for i := range jobs {
			_, _ = e.m.Dispatch(&Echo{Run: e.run, Msg: fmt.Sprint(i)}).Send(ctx)
		}
		var wg sync.WaitGroup
		errs := make(chan error, workers)
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := e.m.Work(ctx, perago.WorkerOptions{StopWhenEmpty: true, Sleep: time.Millisecond}); err != nil {
					errs <- err
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		seen := map[string]int{}
		for _, r := range e.runs() {
			seen[r]++
		}
		if len(seen) != jobs {
			t.Fatalf("ran %d distinct jobs of %d, %s", len(seen), jobs, e.failures())
		}
		for r, n := range seen {
			if n != 1 {
				t.Fatalf("%s ran %d times", r, n)
			}
		}
	})

	t.Run("bulk, size and clear", func(t *testing.T) {
		e := newEnv(t, factory)
		jobs := make([]perago.Job, 0, 10)
		for i := range 10 {
			jobs = append(jobs, &Echo{Run: e.run, Msg: fmt.Sprint(i)})
		}
		if err := e.m.Bulk(ctx, jobs...); err != nil {
			t.Fatal(err)
		}
		if n := e.size("default"); n != 10 {
			t.Fatalf("size = %d", n)
		}
		e.work(perago.WorkerOptions{MaxJobs: 4})
		if got := e.runs(); len(got) != 4 {
			t.Fatalf("max jobs ran %d", len(got))
		}
		if n, err := e.m.Clear(ctx, conn, "default"); err != nil || n != 6 {
			t.Fatalf("clear = %d %v", n, err)
		}
		if n := e.size("default"); n != 0 {
			t.Fatalf("size after clear = %d", n)
		}
	})
}

// ---- jobs ---------------------------------------------------------------------------------

// Every job carries the Run id of its test, so backends can run the suite in parallel.

var state = struct {
	sync.Mutex
	runs   map[string][]string
	counts map[string]int
	failed map[string]int
	inners map[string]func()
}{runs: map[string][]string{}, counts: map[string]int{}, failed: map[string]int{}, inners: map[string]func(){}}

func record(run, s string) {
	state.Lock()
	defer state.Unlock()
	state.runs[run] = append(state.runs[run], s)
}

func take(run string) []string {
	state.Lock()
	defer state.Unlock()
	out := state.runs[run]
	delete(state.runs, run)
	return out
}

func count(run, name string) int {
	state.Lock()
	defer state.Unlock()
	state.counts[run+"/"+name]++
	return state.counts[run+"/"+name]
}

func failedHooks(run string) int {
	state.Lock()
	defer state.Unlock()
	return state.failed[run]
}

func setInner(run string, fn func()) {
	state.Lock()
	defer state.Unlock()
	state.inners[run] = fn
}

// popInner returns the callback once.
func popInner(run string) func() {
	state.Lock()
	defer state.Unlock()
	fn := state.inners[run]
	delete(state.inners, run)
	return fn
}

func forget(run string) {
	state.Lock()
	defer state.Unlock()
	delete(state.runs, run)
	delete(state.failed, run)
	delete(state.inners, run)
	for k := range state.counts {
		if strings.HasPrefix(k, run+"/") {
			delete(state.counts, k)
		}
	}
}

type Echo struct {
	perago.Queueable
	Run  string
	Msg  string
	N    int
	Tags []string
	Meta map[string]int
	At   time.Time
}

func (j *Echo) Handle(context.Context) error {
	if j.N == 0 {
		record(j.Run, "echo "+j.Msg)
		return nil
	}
	record(j.Run, fmt.Sprintf("echo %s %d %v %v %d", j.Msg, j.N, j.Tags, j.Meta, j.At.Unix()))
	return nil
}

type Flaky struct {
	perago.Queueable
	Run       string
	Name      string
	FailTimes int
	TriesN    int
}

func (j *Flaky) Tries() int               { return j.TriesN }
func (j *Flaky) Backoff() []time.Duration { return []time.Duration{10 * time.Second, 30 * time.Second} }
func (j *Flaky) Handle(context.Context) error {
	if count(j.Run, j.Name) <= j.FailTimes {
		return errors.New("boom " + j.Name)
	}
	record(j.Run, fmt.Sprintf("flaky %s attempt %d", j.Name, j.Attempts()))
	return nil
}
func (j *Flaky) Failed(context.Context, error) {
	state.Lock()
	defer state.Unlock()
	state.failed[j.Run]++
}

type Releaser struct {
	perago.Queueable
	Run   string
	Times int
}

func (*Releaser) Tries() int { return 10 }
func (j *Releaser) Handle(context.Context) error {
	if j.Attempts() <= j.Times {
		j.Release(5 * time.Second)
		return nil
	}
	record(j.Run, fmt.Sprintf("released %d", j.Attempts()))
	return nil
}

type Slow struct {
	perago.Queueable
	perago.FailsOnTimeout
	Run string
}

func (*Slow) Timeout() time.Duration { return 50 * time.Millisecond }
func (j *Slow) Handle(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

type UniqueJob struct {
	perago.Queueable
	perago.Unique
	Run string
	Key string
}

func (j *UniqueJob) UniqueID() string         { return j.Run + "/" + j.Key }
func (j *UniqueJob) UniqueFor() time.Duration { return time.Hour }
func (j *UniqueJob) Handle(context.Context) error {
	record(j.Run, "unique "+j.Key)
	return nil
}

const limiterName = "e2e-per-user"

type Limited struct {
	perago.Queueable
	Run  string
	User string
}

// A release uses an attempt, as in Laravel: jobs that may be released need Tries.
func (*Limited) Tries() int { return 5 }
func (j *Limited) Middleware() []perago.Middleware {
	return []perago.Middleware{middleware.RateLimited(limiterName)}
}
func (j *Limited) Handle(context.Context) error {
	record(j.Run, "limited "+j.User)
	return nil
}

type Exclusive struct {
	perago.Queueable
	Run string
	Key string
}

func (*Exclusive) Tries() int { return 5 }
func (j *Exclusive) Middleware() []perago.Middleware {
	return []perago.Middleware{middleware.WithoutOverlapping(j.Run + "/" + j.Key).ReleaseAfter(10 * time.Second).ExpireAfter(time.Minute)}
}
func (j *Exclusive) Handle(context.Context) error {
	record(j.Run, "exclusive "+j.Key)
	if fn := popInner(j.Run); fn != nil {
		fn()
	}
	return nil
}

type Restarter struct {
	perago.Queueable
	Run string
	Msg string
}

func (j *Restarter) Handle(context.Context) error {
	record(j.Run, "restarter "+j.Msg)
	if fn := popInner(j.Run); fn != nil {
		fn()
	}
	return nil
}

type Secret struct {
	perago.Queueable
	perago.Encrypted
	Run  string
	Card string
}

func (*Secret) Tries() int { return 3 } // the test pops it once by hand
func (j *Secret) Handle(context.Context) error {
	record(j.Run, "card "+j.Card)
	return nil
}

func init() {
	for _, j := range []perago.Job{&Echo{}, &Flaky{}, &Releaser{}, &Slow{}, &UniqueJob{}, &Limited{}, &Exclusive{}, &Restarter{}, &Secret{}} {
		perago.Register(j)
	}
}

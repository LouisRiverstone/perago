package perago_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LouisRiverstone/perago"
)

var ctx = context.Background()

type env struct {
	m       *perago.Manager
	q       *perago.MemoryDriver
	clock   *clock
	failed  *perago.MemoryFailedJobStore
	batches *perago.MemoryBatchStore
	cache   *perago.MemoryCache
}

func newEnv(t *testing.T, mutate ...func(*perago.Config)) *env {
	t.Helper()
	flakyCount.Clear()
	takeRuns()
	e := &env{clock: newClock(), failed: perago.NewMemoryFailedJobStore(), batches: perago.NewMemoryBatchStore(), cache: perago.NewMemoryCache()}
	e.q = perago.NewMemoryDriver(90 * time.Second)
	e.q.SetClock(e.clock.Now)
	e.cache.SetClock(e.clock.Now)
	cfg := perago.Config{
		Default: "memory",
		Connections: map[string]perago.Connection{
			"memory":     {Driver: e.q},
			"sync":       {Driver: perago.SyncDriver()},
			"background": {Driver: perago.BackgroundDriver()},
			"deferred":   {Driver: perago.DeferredDriver()},
			"null":       {Driver: perago.Null()},
		},
		Failed: e.failed, Batches: e.batches, Cache: e.cache,
		Key: []byte("0123456789abcdef0123456789abcdef"),
		Now: e.clock.Now,
	}
	for _, fn := range mutate {
		fn(&cfg)
	}
	m, err := perago.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.m = m
	return e
}

// drain runs the worker until the queues are empty, moving the clock past any delay.
func (e *env) drain(t *testing.T, o perago.WorkerOptions) {
	t.Helper()
	o.StopWhenEmpty = true
	o.Sleep = time.Millisecond
	for range 50 {
		if _, err := e.m.Work(ctx, o); err != nil {
			t.Fatal(err)
		}
		queues := o.Queues
		if len(queues) == 0 {
			queues = []string{"default"}
		}
		left := int64(0)
		for _, q := range queues {
			n, _ := e.q.Size(ctx, q)
			left += n
		}
		if left == 0 {
			return
		}
		e.clock.Advance(time.Hour)
	}
	t.Fatal("queue never drained")
}

var (
	logMu sync.Mutex
	runs  []string
)

func record(s string) {
	logMu.Lock()
	defer logMu.Unlock()
	runs = append(runs, s)
}

func takeRuns() []string {
	logMu.Lock()
	defer logMu.Unlock()
	out := runs
	runs = nil
	return out
}

type Echo struct {
	perago.Queueable
	Msg string
}

func (j *Echo) Handle(ctx context.Context) error { record(j.Msg); return nil }

type Flaky struct {
	perago.Queueable
	Name      string
	FailTimes int
	TriesN    int
}

var flakyCount sync.Map

func (j *Flaky) Tries() int { return j.TriesN }
func (j *Flaky) Backoff() []time.Duration {
	return []time.Duration{10 * time.Second, 30 * time.Second}
}
func (j *Flaky) Handle(ctx context.Context) error {
	v, _ := flakyCount.LoadOrStore(j.Name, new(atomic.Int32))
	n := v.(*atomic.Int32).Add(1)
	if int(n) <= j.FailTimes {
		return errors.New("boom " + j.Name)
	}
	record(j.Name)
	return nil
}

var failedCalls atomic.Int32

func (j *Flaky) Failed(ctx context.Context, err error) { failedCalls.Add(1) }

func init() {
	perago.Register(&Echo{})
	perago.Register(&Flaky{})
}

func TestDispatchAndWork(t *testing.T) {
	e := newEnv(t)
	var processed atomic.Int32
	perago.Listen(e.m, func(ctx context.Context, ev perago.JobProcessed) { processed.Add(1) })
	id, err := e.m.Dispatch(&Echo{Msg: "hello"}).Send(ctx)
	if err != nil || id == "" {
		t.Fatalf("dispatch: %q %v", id, err)
	}
	e.drain(t, perago.WorkerOptions{})
	if got := takeRuns(); len(got) != 1 || got[0] != "hello" {
		t.Fatalf("runs = %v", got)
	}
	if processed.Load() != 1 {
		t.Fatalf("JobProcessed fired %d times", processed.Load())
	}
}

func TestRetriesWithBackoffThenSucceeds(t *testing.T) {
	e := newEnv(t)
	var delays []time.Duration
	perago.Listen(e.m, func(ctx context.Context, ev perago.JobReleasedAfterException) { delays = append(delays, ev.Delay) })
	if _, err := e.m.Dispatch(&Flaky{Name: "retry-ok", FailTimes: 2, TriesN: 3}).Send(ctx); err != nil {
		t.Fatal(err)
	}
	e.drain(t, perago.WorkerOptions{})
	if got := takeRuns(); len(got) != 1 {
		t.Fatalf("runs = %v", got)
	}
	if len(delays) != 2 || delays[0] != 10*time.Second || delays[1] != 30*time.Second {
		t.Fatalf("backoff delays = %v", delays)
	}
	if all, _ := e.failed.All(ctx); len(all) != 0 {
		t.Fatalf("failed jobs = %v", all)
	}
}

func TestExhaustedTriesFailsAndStores(t *testing.T) {
	e := newEnv(t)
	failedCalls.Store(0)
	var failedEvents atomic.Int32
	e.m.Failing(func(ctx context.Context, ev perago.JobFailed) { failedEvents.Add(1) })
	if _, err := e.m.Dispatch(&Flaky{Name: "always", FailTimes: 100, TriesN: 2}).OnQueue("q").Send(ctx); err != nil {
		t.Fatal(err)
	}
	e.drain(t, perago.WorkerOptions{Queues: []string{"q"}})
	all, _ := e.failed.All(ctx)
	if len(all) != 1 || !strings.Contains(all[0].Exception, "boom always") || all[0].Queue != "q" || all[0].Connection != "memory" {
		t.Fatalf("failed jobs = %+v", all)
	}
	if failedCalls.Load() != 1 || failedEvents.Load() != 1 {
		t.Fatalf("Failed() %d, JobFailed %d", failedCalls.Load(), failedEvents.Load())
	}
	// queue:retry puts it back with fresh attempts.
	flakyCount.Delete("always")
	if err := e.m.RetryFailed(ctx, all[0].ID); err != nil {
		t.Fatal(err)
	}
	if n, _ := e.q.Size(ctx, "q"); n != 1 {
		t.Fatalf("retried job not queued: %d", n)
	}
	if left, _ := e.failed.All(ctx); len(left) != 0 {
		t.Fatal("retried job still in failed_jobs")
	}
}

func TestWorkerTriesFlagAndUnlimited(t *testing.T) {
	e := newEnv(t)
	if _, err := e.m.Dispatch(&Echo{Msg: "x"}).Send(ctx); err != nil {
		t.Fatal(err)
	}
	e.drain(t, perago.WorkerOptions{Tries: -1})
	takeRuns()
}

type Releaser struct {
	perago.Queueable
	Times int
}

func (j *Releaser) Handle(ctx context.Context) error {
	if j.Attempts() <= j.Times {
		j.Release(5 * time.Second)
		return nil
	}
	record("released-done")
	return nil
}
func (*Releaser) Tries() int { return 10 }

func init() { perago.Register(&Releaser{}) }

func TestManualRelease(t *testing.T) {
	e := newEnv(t)
	if _, err := e.m.Dispatch(&Releaser{Times: 2}).Send(ctx); err != nil {
		t.Fatal(err)
	}
	e.drain(t, perago.WorkerOptions{})
	if got := takeRuns(); len(got) != 1 {
		t.Fatalf("runs = %v", got)
	}
}

type ManualFail struct{ perago.Queueable }

func (j *ManualFail) Handle(ctx context.Context) error { j.Fail("nope"); return nil }

func init() { perago.Register(&ManualFail{}) }

func TestManualFail(t *testing.T) {
	e := newEnv(t)
	_, _ = e.m.Dispatch(&ManualFail{}).Send(ctx)
	e.drain(t, perago.WorkerOptions{Tries: 5})
	all, _ := e.failed.All(ctx)
	if len(all) != 1 || all[0].Exception != "nope" {
		t.Fatalf("failed = %+v", all)
	}
}

type Panicky struct{}

func (Panicky) Handle(context.Context) error { panic("kaboom") }

func init() { perago.Register(&Panicky{}) }

func TestPanicIsAnException(t *testing.T) {
	e := newEnv(t)
	_, _ = e.m.Dispatch(&Panicky{}).Send(ctx)
	e.drain(t, perago.WorkerOptions{})
	all, _ := e.failed.All(ctx)
	if len(all) != 1 || !strings.Contains(all[0].Exception, "kaboom") {
		t.Fatalf("failed = %+v", all)
	}
}

func TestSyncReturnsErrorAndRunsFailed(t *testing.T) {
	e := newEnv(t)
	failedCalls.Store(0)
	err := e.m.DispatchSync(ctx, &Flaky{Name: "sync-fail", FailTimes: 1, TriesN: 3})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("sync error = %v", err)
	}
	if failedCalls.Load() != 1 {
		t.Fatal("Failed() not called for sync job")
	}
	if _, err := e.m.Dispatch(&Echo{Msg: "via-sync"}).OnConnection("sync").Send(ctx); err != nil {
		t.Fatal(err)
	}
	if got := takeRuns(); len(got) != 1 || got[0] != "via-sync" {
		t.Fatalf("runs = %v", got)
	}
}

func TestDelayAndQueueableDefaults(t *testing.T) {
	e := newEnv(t)
	j := &Echo{Msg: "later"}
	j.OnQueue("slow")
	j.DelayFor(time.Minute)
	if _, err := e.m.Dispatch(j).Send(ctx); err != nil {
		t.Fatal(err)
	}
	if code, _ := e.m.Work(ctx, perago.WorkerOptions{Queues: []string{"slow"}, StopWhenEmpty: true}); code != 0 {
		t.Fatal(code)
	}
	if got := takeRuns(); len(got) != 0 {
		t.Fatalf("delayed job ran early: %v", got)
	}
	e.clock.Advance(2 * time.Minute)
	e.drain(t, perago.WorkerOptions{Queues: []string{"slow"}})
	if got := takeRuns(); len(got) != 1 {
		t.Fatalf("runs = %v", got)
	}
	// WithoutDelay ignores the default.
	j2 := &Echo{Msg: "now"}
	j2.DelayFor(time.Hour)
	_, _ = e.m.Dispatch(j2).WithoutDelay().Send(ctx)
	_, _ = e.m.Work(ctx, perago.WorkerOptions{StopWhenEmpty: true})
	if got := takeRuns(); len(got) != 1 {
		t.Fatalf("WithoutDelay: %v", got)
	}
}

func TestQueuePriorities(t *testing.T) {
	e := newEnv(t)
	_, _ = e.m.Dispatch(&Echo{Msg: "low"}).OnQueue("low").Send(ctx)
	_, _ = e.m.Dispatch(&Echo{Msg: "high"}).OnQueue("high").Send(ctx)
	e.drain(t, perago.WorkerOptions{Queues: []string{"high", "low"}})
	if got := takeRuns(); len(got) != 2 || got[0] != "high" {
		t.Fatalf("priority order = %v", got)
	}
}

func TestDispatchIfUnlessAndPrepare(t *testing.T) {
	e := newEnv(t)
	if id, _ := e.m.DispatchIf(ctx, false, &Echo{Msg: "no"}); id != "" {
		t.Fatal("DispatchIf(false) dispatched")
	}
	if id, _ := e.m.DispatchUnless(ctx, true, &Echo{Msg: "no"}); id != "" {
		t.Fatal("DispatchUnless(true) dispatched")
	}
	if id, _ := e.m.Dispatch(&Prepared{OK: false}).Send(ctx); id != "" {
		t.Fatal("PrepareForDispatch false still dispatched")
	}
	if id, _ := e.m.Dispatch(&Prepared{OK: true}).Send(ctx); id == "" {
		t.Fatal("PrepareForDispatch true did not dispatch")
	}
}

type Prepared struct{ OK bool }

func (p *Prepared) Handle(context.Context) error                { return nil }
func (p *Prepared) PrepareForDispatch(ctx context.Context) bool { return p.OK }

type UniqueJob struct {
	perago.Unique
	Key string
}

func (u *UniqueJob) Handle(context.Context) error { record("unique-" + u.Key); return nil }
func (u *UniqueJob) UniqueID() string             { return u.Key }
func (u *UniqueJob) UniqueFor() time.Duration     { return time.Hour }

type UntilProcessing struct {
	perago.UniqueUntilProcessing
	Key string
}

func (u *UntilProcessing) Handle(ctx context.Context) error {
	rt, _ := perago.JobFrom(ctx)
	id, _ := rt.Manager().Dispatch(&UntilProcessing{Key: u.Key}).Send(ctx)
	if id != "" {
		record("redispatched")
	}
	return nil
}
func (u *UntilProcessing) UniqueID() string { return u.Key }

func init() {
	perago.Register(&UniqueJob{})
	perago.Register(&UntilProcessing{})
	perago.Register(&Prepared{})
}

func TestUniqueJobs(t *testing.T) {
	e := newEnv(t)
	first, _ := e.m.Dispatch(&UniqueJob{Key: "a"}).Send(ctx)
	second, _ := e.m.Dispatch(&UniqueJob{Key: "a"}).Send(ctx)
	other, _ := e.m.Dispatch(&UniqueJob{Key: "b"}).Send(ctx)
	if first == "" || second != "" || other == "" {
		t.Fatalf("ids %q %q %q", first, second, other)
	}
	e.drain(t, perago.WorkerOptions{})
	takeRuns()
	if again, _ := e.m.Dispatch(&UniqueJob{Key: "a"}).Send(ctx); again == "" {
		t.Fatal("lock not released after the job ran")
	}
	_, _ = e.q.Clear(ctx, "default")
	_ = e.cache.Forget(ctx, "perago:unique:perago_test.UniqueJob:a")

	// Until-processing: the lock is gone while the job runs.
	_, _ = e.m.Dispatch(&UntilProcessing{Key: "p"}).Send(ctx)
	_, _ = e.m.Work(ctx, perago.WorkerOptions{Once: true})
	if got := takeRuns(); len(got) != 1 || got[0] != "redispatched" {
		t.Fatalf("until processing: %v", got)
	}
}

type Secret struct {
	perago.Encrypted
	Card string
}

func (s *Secret) Handle(context.Context) error { record("card " + s.Card); return nil }

func init() { perago.Register(&Secret{}) }

func TestEncryptedJobs(t *testing.T) {
	e := newEnv(t)
	_, _ = e.m.Dispatch(&Secret{Card: "4111"}).Send(ctx)
	raw := string(e.q.Payloads("default")[0])
	if strings.Contains(raw, "4111") || !strings.Contains(raw, `"secret"`) {
		t.Fatalf("payload not encrypted: %s", raw)
	}
	e.drain(t, perago.WorkerOptions{})
	if got := takeRuns(); len(got) != 1 || got[0] != "card 4111" {
		t.Fatalf("runs = %v", got)
	}

	bad := newEnv(t, func(c *perago.Config) { c.Key = nil })
	if _, err := bad.m.Dispatch(&Secret{Card: "1"}).Send(ctx); perago.CodeOf(err) != perago.InvalidConfig {
		t.Fatalf("missing key error = %v", err)
	}
}

func TestChainRunsInOrderAndStopsOnFailure(t *testing.T) {
	e := newEnv(t)
	if _, err := e.m.Chain(&Echo{Msg: "1"}, &Echo{Msg: "2"}, &Echo{Msg: "3"}).Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	e.drain(t, perago.WorkerOptions{})
	if got := strings.Join(takeRuns(), ","); got != "1,2,3" {
		t.Fatalf("chain order = %s", got)
	}

	var caught atomic.Value
	_, err := e.m.Chain(&Echo{Msg: "a"}, &Flaky{Name: "chain-fail", FailTimes: 9, TriesN: 1}, &Echo{Msg: "never"}).
		OnConnection("memory").OnQueue("chain").
		Catch(func(ctx context.Context, err error) { caught.Store(err.Error()) }).
		Dispatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	e.drain(t, perago.WorkerOptions{Queues: []string{"chain"}})
	if got := strings.Join(takeRuns(), ","); got != "a" {
		t.Fatalf("chain after failure = %s", got)
	}
	if v, _ := caught.Load().(string); !strings.Contains(v, "boom chain-fail") {
		t.Fatalf("catch got %q", v)
	}
}

type Prepender struct{ perago.Queueable }

func (j *Prepender) Handle(context.Context) error {
	record("p")
	if err := j.PrependToChain(&Echo{Msg: "prepended"}); err != nil {
		return err
	}
	return j.AppendToChain(&Echo{Msg: "appended"})
}

func init() { perago.Register(&Prepender{}) }

func TestPrependAppendToChain(t *testing.T) {
	e := newEnv(t)
	_, _ = e.m.Chain(&Prepender{}, &Echo{Msg: "orig"}).Dispatch(ctx)
	e.drain(t, perago.WorkerOptions{})
	if got := strings.Join(takeRuns(), ","); got != "p,prepended,orig,appended" {
		t.Fatalf("chain = %s", got)
	}
}

func TestBatchCallbacks(t *testing.T) {
	e := newEnv(t)
	var then, finally, catch, progress atomic.Int32
	var before atomic.Int32
	b, err := e.m.Batch(&Echo{Msg: "b1"}, &Echo{Msg: "b2"}, []perago.Job{&Echo{Msg: "c1"}, &Echo{Msg: "c2"}}).
		Name("import").
		Before(func(ctx context.Context, b *perago.Batch) { before.Add(1) }).
		Progress(func(ctx context.Context, b *perago.Batch) { progress.Add(1) }).
		Then(func(ctx context.Context, b *perago.Batch) { then.Add(1) }).
		Catch(func(ctx context.Context, b *perago.Batch, err error) { catch.Add(1) }).
		Finally(func(ctx context.Context, b *perago.Batch) { finally.Add(1) }).
		Dispatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if b.TotalJobs != 4 || b.Name != "import" || before.Load() != 1 {
		t.Fatalf("batch = %+v before=%d", b, before.Load())
	}
	e.drain(t, perago.WorkerOptions{})
	if got := takeRuns(); len(got) != 4 {
		t.Fatalf("runs = %v", got)
	}
	fresh, err := e.m.FindBatch(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !fresh.Finished() || fresh.PendingJobs != 0 || fresh.Progress() != 100 {
		t.Fatalf("batch after run = %+v", fresh)
	}
	if then.Load() != 1 || finally.Load() != 1 || catch.Load() != 0 || progress.Load() != 4 {
		t.Fatalf("then=%d finally=%d catch=%d progress=%d", then.Load(), finally.Load(), catch.Load(), progress.Load())
	}
}

func TestBatchFailureCancelsUnlessAllowed(t *testing.T) {
	e := newEnv(t)
	var catch, finally, then atomic.Int32
	b, err := e.m.Batch(&Flaky{Name: "bf", FailTimes: 9, TriesN: 1}, &Echo{Msg: "ok"}).
		Catch(func(ctx context.Context, b *perago.Batch, err error) { catch.Add(1) }).
		Then(func(ctx context.Context, b *perago.Batch) { then.Add(1) }).
		Finally(func(ctx context.Context, b *perago.Batch) { finally.Add(1) }).
		Dispatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	e.drain(t, perago.WorkerOptions{})
	takeRuns()
	fresh, _ := e.m.FindBatch(ctx, b.ID)
	if !fresh.Cancelled() || fresh.FailedJobs != 1 || len(fresh.FailedJobIDs) != 1 {
		t.Fatalf("batch = %+v", fresh)
	}
	if catch.Load() != 1 || then.Load() != 0 || finally.Load() != 1 {
		t.Fatalf("catch=%d then=%d finally=%d", catch.Load(), then.Load(), finally.Load())
	}

	// queue:retry-batch with the failure fixed finishes the batch.
	flakyCount.Delete("bf")
	b2, _ := e.m.Batch(&Flaky{Name: "bf2", FailTimes: 1, TriesN: 1}).AllowFailures().
		Then(func(ctx context.Context, b *perago.Batch) { then.Add(1) }).Dispatch(ctx)
	e.drain(t, perago.WorkerOptions{})
	mid, _ := e.m.FindBatch(ctx, b2.ID)
	if mid.Cancelled() || mid.FailedJobs != 1 {
		t.Fatalf("allowFailures batch = %+v", mid)
	}
	if err := e.m.RetryBatch(ctx, b2.ID); err != nil {
		t.Fatal(err)
	}
	e.drain(t, perago.WorkerOptions{})
	takeRuns()
	done, _ := e.m.FindBatch(ctx, b2.ID)
	if !done.Finished() || len(done.FailedJobIDs) != 0 || then.Load() != 1 {
		t.Fatalf("retried batch = %+v then=%d", done, then.Load())
	}
}

type Adder struct{ perago.Queueable }

func (j *Adder) Handle(ctx context.Context) error {
	b, err := j.Batch(ctx)
	if err != nil || b == nil {
		return errors.New("no batch")
	}
	if b.Cancelled() {
		return nil
	}
	return b.Add(ctx, &Echo{Msg: "added"})
}

func init() { perago.Register(&Adder{}) }

func TestAddJobsToBatchAndChainedBatch(t *testing.T) {
	e := newEnv(t)
	var then atomic.Int32
	b, err := e.m.Batch(&Adder{}).Then(func(ctx context.Context, b *perago.Batch) { then.Add(1) }).Dispatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	e.drain(t, perago.WorkerOptions{})
	if got := takeRuns(); len(got) != 1 || got[0] != "added" {
		t.Fatalf("runs = %v", got)
	}
	fresh, _ := e.m.FindBatch(ctx, b.ID)
	if fresh.TotalJobs != 2 || !fresh.Finished() || then.Load() != 1 {
		t.Fatalf("batch = %+v then=%d", fresh, then.Load())
	}

	_, err = e.m.Chain(&Echo{Msg: "before"}, e.m.Batch(&Echo{Msg: "in1"}, &Echo{Msg: "in2"}), &Echo{Msg: "after"}).Dispatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	e.drain(t, perago.WorkerOptions{})
	got := takeRuns()
	if len(got) != 4 || got[0] != "before" || got[3] != "after" {
		t.Fatalf("chained batch = %v", got)
	}
}

func TestBatchNeedsStore(t *testing.T) {
	e := newEnv(t, func(c *perago.Config) { c.Batches = nil })
	if _, err := e.m.Batch(&Echo{}).Dispatch(ctx); perago.CodeOf(err) != perago.InvalidConfig {
		t.Fatalf("err = %v", err)
	}
}

func TestAfterCommit(t *testing.T) {
	e := newEnv(t, func(c *perago.Config) {
		conn := c.Connections["memory"]
		conn.AfterCommit = true
		c.Connections["memory"] = conn
	})
	_ = e.m.Transaction(ctx, func(ctx context.Context) error {
		_, _ = e.m.Dispatch(&Echo{Msg: "committed"}).Send(ctx)
		if n, _ := e.q.Size(ctx, "default"); n != 0 {
			t.Fatal("job pushed before commit")
		}
		return nil
	})
	if n, _ := e.q.Size(ctx, "default"); n != 1 {
		t.Fatalf("after commit size = %d", n)
	}
	_ = e.m.Transaction(ctx, func(ctx context.Context) error {
		_, _ = e.m.Dispatch(&Echo{Msg: "rolled back"}).Send(ctx)
		_, _ = e.m.Dispatch(&Echo{Msg: "before commit"}).BeforeCommit().Send(ctx)
		return errors.New("rollback")
	})
	if n, _ := e.q.Size(ctx, "default"); n != 2 {
		t.Fatalf("after rollback size = %d", n)
	}
}

func TestRouteAndForward(t *testing.T) {
	e := newEnv(t)
	e.m.Route(&Echo{}, "", "echoes")
	_, _ = e.m.Dispatch(&Echo{Msg: "routed"}).Send(ctx)
	if n, _ := e.q.Size(ctx, "echoes"); n != 1 {
		t.Fatal("route ignored")
	}
	_, _ = e.m.Dispatch(&Echo{Msg: "explicit"}).OnQueue("mine").Send(ctx)
	if n, _ := e.q.Size(ctx, "mine"); n != 1 {
		t.Fatal("explicit queue lost to route")
	}
	e.m.Forward("mine", "mine.v2", "")
	_, _ = e.m.Dispatch(&Echo{Msg: "fwd"}).OnQueue("mine").Send(ctx)
	if n, _ := e.q.Size(ctx, "mine.v2"); n != 1 {
		t.Fatal("forward ignored")
	}
	perago.RouteInterface[interface{ Tries() int }](e.m, "null", "")
	_, _ = e.m.Dispatch(&Flaky{Name: "to-null"}).Send(ctx)
	if n, _ := e.q.Size(ctx, "default"); n != 0 {
		t.Fatal("interface route ignored")
	}
}

type brokenDriver struct{ perago.Driver }

func (brokenDriver) Push(context.Context, perago.Message) (string, error) {
	return "", errors.New("down")
}

func TestFailover(t *testing.T) {
	e := newEnv(t, func(c *perago.Config) {
		c.Connections["broken"] = perago.Connection{Driver: brokenDriver{perago.Null()}}
		c.Connections["failover"] = perago.Connection{Driver: perago.FailoverDriver("broken", "memory")}
	})
	var failedOver atomic.Int32
	perago.Listen(e.m, func(ctx context.Context, ev perago.QueueFailedOver) { failedOver.Add(1) })
	if _, err := e.m.Dispatch(&Echo{Msg: "fo"}).OnConnection("failover").Send(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := e.q.Size(ctx, "default"); n != 1 || failedOver.Load() != 1 {
		t.Fatalf("size=%d failedOver=%d", n, failedOver.Load())
	}
	if _, err := perago.New(perago.Config{Default: "f", Connections: map[string]perago.Connection{"f": {Driver: perago.FailoverDriver("nope")}}}); err == nil {
		t.Fatal("failover to an unknown connection accepted")
	}
}

type remoteDriver struct{ perago.Driver }

func TestClosuresAndBackgroundDeferred(t *testing.T) {
	e := newEnv(t, func(c *perago.Config) {
		c.Connections["remote"] = perago.Connection{Driver: remoteDriver{perago.NewMemoryDriver(0)}}
	})
	var n atomic.Int32
	if _, err := e.m.Dispatch(perago.Closure(func(ctx context.Context) error { n.Add(1); return nil })).OnConnection("background").Send(ctx); err != nil {
		t.Fatal(err)
	}
	e.m.Wait()
	if _, err := e.m.Dispatch(perago.Closure(func(ctx context.Context) error { return nil })).OnConnection("remote").Send(ctx); perago.CodeOf(err) != perago.ClosureNotSerializable {
		t.Fatalf("closure on a remote connection: %v", err)
	}
	dctx, flush := perago.WithDeferred(ctx)
	_ = e.m.DispatchAfterResponse(dctx, perago.Closure(func(ctx context.Context) error { n.Add(10); return nil }))
	_, _ = e.m.Dispatch(perago.Closure(func(ctx context.Context) error { n.Add(100); return nil })).OnConnection("deferred").Send(dctx)
	if n.Load() != 1 {
		t.Fatal("deferred ran before flush")
	}
	flush()
	if n.Load() != 111 {
		t.Fatalf("n = %d", n.Load())
	}

	perago.RegisterClosure("greet", func(ctx context.Context, args json.RawMessage) error { record("hi " + string(args)); return nil })
	_, _ = e.m.Dispatch(perago.Call("greet", "bob")).Send(ctx)
	e.drain(t, perago.WorkerOptions{})
	if got := takeRuns(); len(got) != 1 || got[0] != `hi "bob"` {
		t.Fatalf("named closure = %v", got)
	}
}

type Slow struct{ FailOn bool }

func (s *Slow) Handle(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}
func (s *Slow) Timeout() time.Duration { return 20 * time.Millisecond }
func (s *Slow) FailOnTimeout() bool    { return s.FailOn }

type Stuck struct{}

func (Stuck) Handle(context.Context) error { time.Sleep(300 * time.Millisecond); return nil }
func (Stuck) Timeout() time.Duration       { return 10 * time.Millisecond }

func init() {
	perago.Register(&Slow{})
	perago.Register(&Stuck{})
}

func TestTimeouts(t *testing.T) {
	e := newEnv(t)
	var timedOut atomic.Int32
	perago.Listen(e.m, func(ctx context.Context, ev perago.JobTimedOut) { timedOut.Add(1) })
	_, _ = e.m.Dispatch(&Slow{FailOn: true}).Send(ctx)
	e.drain(t, perago.WorkerOptions{Tries: 5})
	all, _ := e.failed.All(ctx)
	if len(all) != 1 || !strings.Contains(all[0].Exception, "timed out") || timedOut.Load() != 1 {
		t.Fatalf("failed = %+v timedOut=%d", all, timedOut.Load())
	}
	_, _ = e.m.Dispatch(&Stuck{}).Send(ctx)
	code, err := e.m.Work(ctx, perago.WorkerOptions{StopWhenEmpty: true, Grace: 10 * time.Millisecond})
	if code != perago.ExitError || perago.CodeOf(err) != perago.JobTimeout {
		t.Fatalf("stuck job: code=%d err=%v", code, err)
	}
}

type MaxEx struct {
	perago.Queueable
	Name string
}

func (j *MaxEx) Handle(context.Context) error {
	if j.Attempts()%2 == 1 {
		return errors.New("odd")
	}
	j.Release(0)
	return nil
}
func (j *MaxEx) Tries() int         { return 0 }
func (j *MaxEx) MaxExceptions() int { return 2 }

func init() { perago.Register(&MaxEx{}) }

func TestMaxExceptions(t *testing.T) {
	e := newEnv(t)
	_, _ = e.m.Dispatch(&MaxEx{}).Send(ctx)
	e.drain(t, perago.WorkerOptions{})
	all, _ := e.failed.All(ctx)
	if len(all) != 1 || !strings.Contains(all[0].Exception, "MAX_EXCEPTIONS_EXCEEDED") {
		t.Fatalf("failed = %+v", all)
	}
}

type Deadline struct{ Until time.Time }

func (d *Deadline) Handle(context.Context) error { return errors.New("try again") }
func (d *Deadline) RetryUntil() time.Time        { return d.Until }

func init() { perago.Register(&Deadline{}) }

func TestRetryUntilWinsOverTries(t *testing.T) {
	e := newEnv(t)
	var released atomic.Int32
	perago.Listen(e.m, func(ctx context.Context, ev perago.JobReleasedAfterException) {
		released.Add(1)
		e.clock.Advance(10 * time.Minute)
	})
	_, _ = e.m.Dispatch(&Deadline{Until: e.clock.Now().Add(25 * time.Minute)}).Send(ctx)
	e.drain(t, perago.WorkerOptions{Tries: 1})
	if released.Load() < 2 {
		t.Fatalf("retryUntil ignored, released %d times", released.Load())
	}
	if all, _ := e.failed.All(ctx); len(all) != 1 {
		t.Fatal("job never failed after its deadline")
	}
}

var errPermanent = errors.New("permanent")

type Permanent struct{}

func (Permanent) Handle(context.Context) error { return errPermanent }
func (Permanent) Tries() int                   { return 10 }

func init() { perago.Register(&Permanent{}) }

func TestDontRetry(t *testing.T) {
	e := newEnv(t)
	e.m.DontRetry(errPermanent)
	var attempts atomic.Int32
	perago.Listen(e.m, func(ctx context.Context, ev perago.JobAttempted) { attempts.Add(1) })
	_, _ = e.m.Dispatch(&Permanent{}).Send(ctx)
	e.drain(t, perago.WorkerOptions{})
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d", attempts.Load())
	}
}

type Middled struct{ perago.Queueable }

func (j *Middled) Handle(context.Context) error { record("handle"); return nil }
func (j *Middled) Middleware() []perago.Middleware {
	mw := func(name string) perago.Middleware {
		return perago.MiddlewareFunc(func(ctx context.Context, job perago.Job, next perago.Next) error {
			record(name + ">")
			err := next(ctx)
			record("<" + name)
			return err
		})
	}
	return []perago.Middleware{mw("a"), mw("b")}
}

func init() { perago.Register(&Middled{}) }

func TestMiddlewareOrder(t *testing.T) {
	e := newEnv(t)
	_, _ = e.m.Dispatch(&Middled{}).Send(ctx)
	e.drain(t, perago.WorkerOptions{})
	if got := strings.Join(takeRuns(), " "); got != "a> b> handle <b <a" {
		t.Fatalf("middleware = %s", got)
	}
}

func TestRestartPauseAndStopConditions(t *testing.T) {
	e := newEnv(t)
	for i := range 3 {
		_, _ = e.m.Dispatch(&Echo{Msg: string(rune('a' + i))}).Send(ctx)
	}
	if code, _ := e.m.Work(ctx, perago.WorkerOptions{MaxJobs: 2}); code != 0 {
		t.Fatal(code)
	}
	if got := takeRuns(); len(got) != 2 {
		t.Fatalf("max jobs = %v", got)
	}
	if err := e.m.Pause(ctx, "", "", 0); err != nil {
		t.Fatal(err)
	}
	_, _ = e.m.Work(ctx, perago.WorkerOptions{StopWhenEmpty: true})
	if got := takeRuns(); len(got) != 0 {
		t.Fatal("paused queue processed")
	}
	_ = e.m.Resume(ctx, "", "")
	e.clock.Advance(time.Second)
	if err := e.m.Restart(ctx); err != nil {
		t.Fatal(err)
	}
	// The restart signal predates this worker, so it keeps going.
	_, _ = e.m.Work(ctx, perago.WorkerOptions{StopWhenEmpty: true})
	if got := takeRuns(); len(got) != 1 {
		t.Fatalf("after resume = %v", got)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if code, _ := e.m.Work(cctx, perago.WorkerOptions{}); code != 0 {
		t.Fatal("cancelled context did not stop the worker")
	}
	if _, err := e.m.Work(ctx, perago.WorkerOptions{Connection: "sync"}); err == nil {
		t.Fatal("sync connection accepted a worker")
	}
}

type Restartable struct{}

func (Restartable) Handle(ctx context.Context) error {
	rt, _ := perago.JobFrom(ctx)
	return rt.Manager().Restart(ctx)
}

func init() { perago.Register(&Restartable{}) }

func TestRestartStopsRunningWorker(t *testing.T) {
	e := newEnv(t)
	_, _ = e.m.Dispatch(&Restartable{}).Send(ctx)
	_, _ = e.m.Dispatch(&Echo{Msg: "after"}).Send(ctx)
	e.clock.Advance(time.Second)
	done := make(chan int, 1)
	go func() {
		code, _ := e.m.Work(ctx, perago.WorkerOptions{Sleep: time.Millisecond})
		done <- code
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker ignored queue:restart")
	}
	if got := takeRuns(); len(got) != 0 {
		t.Fatalf("worker kept processing after restart: %v", got)
	}
}

type Debounced struct {
	perago.Queueable
	N int
}

func (d *Debounced) Handle(context.Context) error { record("debounced"); return nil }
func (d *Debounced) DebounceFor() time.Duration   { return 30 * time.Second }
func (d *Debounced) DebounceID() string           { return "doc-1" }

func init() { perago.Register(&Debounced{}) }

func TestDebounce(t *testing.T) {
	e := newEnv(t)
	var superseded atomic.Int32
	perago.Listen(e.m, func(ctx context.Context, ev perago.JobDebounced) { superseded.Add(1) })
	for i := range 3 {
		_, _ = e.m.Dispatch(&Debounced{N: i}).Send(ctx)
	}
	e.drain(t, perago.WorkerOptions{})
	if got := takeRuns(); len(got) != 1 || superseded.Load() != 2 {
		t.Fatalf("runs=%v superseded=%d", got, superseded.Load())
	}
}

func TestBulk(t *testing.T) {
	e := newEnv(t)
	if err := e.m.Bulk(ctx, &Echo{Msg: "1"}, &Echo{Msg: "2"}, &Echo{Msg: "3"}); err != nil {
		t.Fatal(err)
	}
	if n, _ := e.q.Size(ctx, "default"); n != 3 {
		t.Fatalf("size = %d", n)
	}
}

func TestDefaultConnectionFromEnv(t *testing.T) {
	t.Setenv("QUEUE_CONNECTION", "b")
	m, err := perago.New(perago.Config{Connections: map[string]perago.Connection{"a": {Driver: perago.Null()}, "b": {Driver: perago.Null()}}})
	if err != nil || m.DefaultConnection() != "b" {
		t.Fatalf("default = %v %v", m, err)
	}
	if _, err := perago.New(perago.Config{Default: "x", Connections: map[string]perago.Connection{"a": {Driver: perago.Null()}}}); perago.CodeOf(err) != perago.ConnectionNotFound {
		t.Fatalf("unknown default: %v", err)
	}
}

func TestUnregisteredJobFails(t *testing.T) {
	e := newEnv(t)
	_, _ = e.q.Push(ctx, perago.Message{Queue: "default", Payload: []byte(`{"uuid":"u1","job":"nope.Missing","data":{}}`)})
	_, _ = e.q.Push(ctx, perago.Message{Queue: "default", Payload: []byte(`not json`)})
	e.drain(t, perago.WorkerOptions{})
	all, _ := e.failed.All(ctx)
	if len(all) != 2 {
		t.Fatalf("failed = %+v", all)
	}
}

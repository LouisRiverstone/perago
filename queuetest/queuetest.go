// Package queuetest is Laravel's Queue::fake and Bus::fake for perago: capture dispatches instead
// of queueing them, then assert on what was pushed, chained and batched; or run a job with a fake
// runtime and assert what it did to the queue (released, deleted, failed).
//
//	fake := queuetest.Fake(m)
//	defer fake.Restore()
//	service.Ship(ctx, order)
//	queuetest.AssertPushed(t, fake, func(j *ShipOrder) bool { return j.OrderID == order.ID })
package queuetest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LouisRiverstone/perago"
)

// Pushed is one captured dispatch.
type Pushed struct {
	Job           perago.Job
	Connection    string
	Queue         string
	Delay         time.Duration
	Chain         []any
	Sync          bool
	AfterResponse bool
}

// Batched is one captured batch.
type Batched struct {
	Name          string
	Jobs          []any
	Connection    string
	Queue         string
	AllowFailures bool
	Batch         *perago.Batch
}

// HasJobs reports whether the batch holds jobs matching each want in order: a job value or
// instance compares by type, a func(T) bool by predicate.
func (b Batched) HasJobs(want ...any) bool {
	if len(b.Jobs) != len(want) {
		return false
	}
	for i, w := range want {
		if !matchItem(b.Jobs[i], w) {
			return false
		}
	}
	return true
}

// FakeQueue records dispatches instead of queueing them.
type FakeQueue struct {
	mu      sync.Mutex
	pushed  []Pushed
	batched []Batched
	only    []reflect.Type
	except  []reflect.Type
	restore func()
}

// Fake captures every dispatch of m (Queue::fake / Bus::fake).
func Fake(m *perago.Manager) *FakeQueue { return install(m, &FakeQueue{}) }

// FakeFor captures only jobs of the given types; the rest are dispatched for real.
func FakeFor(m *perago.Manager, jobs ...perago.Job) *FakeQueue {
	return install(m, &FakeQueue{only: typesOf(jobs)})
}

// FakeExcept captures every job except those of the given types.
func FakeExcept(m *perago.Manager, jobs ...perago.Job) *FakeQueue {
	return install(m, &FakeQueue{except: typesOf(jobs)})
}

func install(m *perago.Manager, f *FakeQueue) *FakeQueue {
	f.restore = m.Intercept(f)
	return f
}

func typesOf(jobs []perago.Job) []reflect.Type {
	out := make([]reflect.Type, len(jobs))
	for i, j := range jobs {
		out[i] = base(j)
	}
	return out
}

func base(v any) reflect.Type {
	t := reflect.TypeOf(v)
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

// Restore removes the fake.
func (f *FakeQueue) Restore() { f.restore() }

func (f *FakeQueue) fakes(job perago.Job) bool {
	t := base(job)
	for _, x := range f.except {
		if x == t {
			return false
		}
	}
	if len(f.only) == 0 {
		return true
	}
	for _, x := range f.only {
		if x == t {
			return true
		}
	}
	return false
}

func (f *FakeQueue) InterceptDispatch(_ context.Context, d perago.Dispatched) bool {
	if !f.fakes(d.Job) {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pushed = append(f.pushed, Pushed(d))
	return true
}

func (f *FakeQueue) InterceptBatch(_ context.Context, b *perago.PendingBatch) (*perago.Batch, bool) {
	conn, queue := b.Target()
	batch, _ := perago.NewFakeBatch(perago.NewUUID(), b.BatchName())
	batch.TotalJobs, batch.PendingJobs = len(b.Jobs()), len(b.Jobs())
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batched = append(f.batched, Batched{Name: b.BatchName(), Jobs: b.Jobs(), Connection: conn, Queue: queue, AllowFailures: b.AllowsFailures(), Batch: batch})
	return batch, true
}

// Pushed returns every captured dispatch.
func (f *FakeQueue) Pushed() []Pushed {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Pushed(nil), f.pushed...)
}

// Batched returns every captured batch.
func (f *FakeQueue) Batched() []Batched {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Batched(nil), f.batched...)
}

func pushedOf[T perago.Job](f *FakeQueue, filters []func(T) bool) []Pushed {
	var out []Pushed
	for _, p := range f.Pushed() {
		j, ok := p.Job.(T)
		if !ok || !all(j, filters) {
			continue
		}
		out = append(out, p)
	}
	return out
}

func all[T any](v T, filters []func(T) bool) bool {
	for _, fn := range filters {
		if !fn(v) {
			return false
		}
	}
	return true
}

func name[T any]() string { return reflect.TypeFor[T]().String() }

// AssertPushed fails unless a job of type T matching every filter was pushed.
func AssertPushed[T perago.Job](t testing.TB, f *FakeQueue, filters ...func(T) bool) {
	t.Helper()
	if len(pushedOf(f, filters)) == 0 {
		t.Fatalf("expected %s to be pushed; pushed: %s", name[T](), f.summary())
	}
}

// AssertPushedTimes fails unless jobs of type T were pushed exactly n times.
func AssertPushedTimes[T perago.Job](t testing.TB, f *FakeQueue, n int, filters ...func(T) bool) {
	t.Helper()
	if got := len(pushedOf(f, filters)); got != n {
		t.Fatalf("expected %s to be pushed %d times, got %d", name[T](), n, got)
	}
}

// AssertPushedOnce is AssertPushedTimes with 1.
func AssertPushedOnce[T perago.Job](t testing.TB, f *FakeQueue, filters ...func(T) bool) {
	t.Helper()
	AssertPushedTimes(t, f, 1, filters...)
}

// AssertPushedOn fails unless a job of type T was pushed on queue.
func AssertPushedOn[T perago.Job](t testing.TB, f *FakeQueue, queue string, filters ...func(T) bool) {
	t.Helper()
	for _, p := range pushedOf(f, filters) {
		if p.Queue == queue {
			return
		}
	}
	t.Fatalf("expected %s to be pushed on queue %q; pushed: %s", name[T](), queue, f.summary())
}

// AssertNotPushed fails when a job of type T matching every filter was pushed.
func AssertNotPushed[T perago.Job](t testing.TB, f *FakeQueue, filters ...func(T) bool) {
	t.Helper()
	if n := len(pushedOf(f, filters)); n > 0 {
		t.Fatalf("expected %s not to be pushed, it was pushed %d times", name[T](), n)
	}
}

// AssertNothingPushed fails when any job was pushed.
func AssertNothingPushed(t testing.TB, f *FakeQueue) {
	t.Helper()
	if n := len(f.Pushed()); n > 0 {
		t.Fatalf("expected nothing pushed, got %d: %s", n, f.summary())
	}
}

// AssertCount fails unless exactly n jobs were pushed.
func AssertCount(t testing.TB, f *FakeQueue, n int) {
	t.Helper()
	if got := len(f.Pushed()); got != n {
		t.Fatalf("expected %d pushed jobs, got %d: %s", n, got, f.summary())
	}
}

func isClosure(j perago.Job) bool {
	return perago.NameOf(j) == "perago.Closure" || perago.NameOf(j) == "perago.Call"
}

// AssertClosurePushed fails unless a closure job was pushed.
func AssertClosurePushed(t testing.TB, f *FakeQueue) {
	t.Helper()
	for _, p := range f.Pushed() {
		if isClosure(p.Job) {
			return
		}
	}
	t.Fatal("expected a closure to be pushed")
}

// AssertClosureNotPushed fails when a closure job was pushed.
func AssertClosureNotPushed(t testing.TB, f *FakeQueue) {
	t.Helper()
	for _, p := range f.Pushed() {
		if isClosure(p.Job) {
			t.Fatal("expected no closure to be pushed")
		}
	}
}

// AssertDispatchedSync fails unless a job of type T was dispatched with DispatchSync.
func AssertDispatchedSync[T perago.Job](t testing.TB, f *FakeQueue, filters ...func(T) bool) {
	t.Helper()
	for _, p := range pushedOf(f, filters) {
		if p.Sync {
			return
		}
	}
	t.Fatalf("expected %s to be dispatched synchronously", name[T]())
}

// AssertDispatchedAfterResponse fails unless a job of type T was dispatched after the response.
func AssertDispatchedAfterResponse[T perago.Job](t testing.TB, f *FakeQueue, filters ...func(T) bool) {
	t.Helper()
	for _, p := range pushedOf(f, filters) {
		if p.AfterResponse {
			return
		}
	}
	t.Fatalf("expected %s to be dispatched after the response", name[T]())
}

// ChainedBatch matches a batch inside a chain for AssertChained.
func ChainedBatch(fn func(b Batched) bool) any { return chainedBatchMatcher(fn) }

type chainedBatchMatcher func(b Batched) bool

func matchItem(got, want any) bool {
	if m, ok := want.(chainedBatchMatcher); ok {
		pb, isBatch := got.(*perago.PendingBatch)
		if !isBatch {
			return false
		}
		conn, queue := pb.Target()
		return m(Batched{Name: pb.BatchName(), Jobs: pb.Jobs(), Connection: conn, Queue: queue, AllowFailures: pb.AllowsFailures()})
	}
	wv := reflect.ValueOf(want)
	if wv.Kind() == reflect.Func && wv.Type().NumIn() == 1 && wv.Type().NumOut() == 1 && wv.Type().Out(0).Kind() == reflect.Bool {
		gv := reflect.ValueOf(got)
		if !gv.IsValid() || !gv.Type().AssignableTo(wv.Type().In(0)) {
			return false
		}
		return wv.Call([]reflect.Value{gv})[0].Bool()
	}
	if js, ok := want.([]perago.Job); ok {
		gs, ok := got.([]perago.Job)
		if !ok || len(gs) != len(js) {
			return false
		}
		for i := range js {
			if base(gs[i]) != base(js[i]) {
				return false
			}
		}
		return true
	}
	return base(got) == base(want)
}

// AssertChained fails unless a chain was dispatched whose jobs match want in order (a job value
// compares by type, a func(T) bool by predicate, ChainedBatch matches a batch).
func AssertChained(t testing.TB, f *FakeQueue, want ...any) {
	t.Helper()
	for _, p := range f.Pushed() {
		items := append([]any{p.Job}, p.Chain...)
		if len(items) != len(want) {
			continue
		}
		ok := true
		for i := range want {
			if !matchItem(items[i], want[i]) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
	}
	t.Fatalf("expected a chain of %d jobs matching; pushed: %s", len(want), f.summary())
}

// AssertDispatchedWithoutChain fails unless a job of type T was dispatched with no chain.
func AssertDispatchedWithoutChain[T perago.Job](t testing.TB, f *FakeQueue, filters ...func(T) bool) {
	t.Helper()
	for _, p := range pushedOf(f, filters) {
		if len(p.Chain) == 0 {
			return
		}
	}
	t.Fatalf("expected %s to be dispatched without a chain", name[T]())
}

// AssertBatched fails unless a batch matching fn was dispatched.
func AssertBatched(t testing.TB, f *FakeQueue, fn func(b Batched) bool) {
	t.Helper()
	for _, b := range f.Batched() {
		if fn(b) {
			return
		}
	}
	t.Fatalf("expected a matching batch; %d batches dispatched", len(f.Batched()))
}

// AssertBatchCount fails unless exactly n batches were dispatched.
func AssertBatchCount(t testing.TB, f *FakeQueue, n int) {
	t.Helper()
	if got := len(f.Batched()); got != n {
		t.Fatalf("expected %d batches, got %d", n, got)
	}
}

// AssertNothingBatched fails when a batch was dispatched.
func AssertNothingBatched(t testing.TB, f *FakeQueue) {
	t.Helper()
	AssertBatchCount(t, f, 0)
}

func (f *FakeQueue) summary() string {
	ps := f.Pushed()
	if len(ps) == 0 {
		return "nothing"
	}
	names := make([]string, len(ps))
	for i, p := range ps {
		names[i] = fmt.Sprintf("%s@%s:%s", perago.DisplayNameOf(p.Job), p.Connection, p.Queue)
	}
	return strings.Join(names, ", ")
}

// Interactions is a job bound to a fake runtime (withFakeQueueInteractions).
type Interactions struct {
	Runtime *perago.Runtime
	job     perago.Job
}

// WithFakeQueueInteractions binds job to a runtime that records Release, Delete, Fail and chain
// changes. attempts sets Attempts() (1 when omitted). Call Handle on the returned value.
func WithFakeQueueInteractions(job perago.Job, attempts ...int) *Interactions {
	n := 1
	if len(attempts) > 0 {
		n = attempts[0]
	}
	return &Interactions{Runtime: perago.NewFakeRuntime(job, n), job: job}
}

// Handle runs the job with the fake runtime in its context.
func (i *Interactions) Handle(ctx context.Context) error {
	return i.job.Handle(perago.BindRuntime(ctx, i.Runtime))
}

// AssertReleased fails unless the job released itself, with delay when given.
func (i *Interactions) AssertReleased(t testing.TB, delay ...time.Duration) {
	t.Helper()
	ok, d := i.Runtime.IsReleased()
	if !ok {
		t.Fatal("expected the job to be released")
	}
	if len(delay) > 0 && d != delay[0] {
		t.Fatalf("expected a release delay of %s, got %s", delay[0], d)
	}
}

// AssertNotReleased fails when the job released itself.
func (i *Interactions) AssertNotReleased(t testing.TB) {
	t.Helper()
	if ok, _ := i.Runtime.IsReleased(); ok {
		t.Fatal("expected the job not to be released")
	}
}

// AssertDeleted fails unless the job deleted itself.
func (i *Interactions) AssertDeleted(t testing.TB) {
	t.Helper()
	if !i.Runtime.IsDeleted() {
		t.Fatal("expected the job to be deleted")
	}
}

// AssertNotDeleted fails when the job deleted itself.
func (i *Interactions) AssertNotDeleted(t testing.TB) {
	t.Helper()
	if i.Runtime.IsDeleted() {
		t.Fatal("expected the job not to be deleted")
	}
}

// AssertFailed fails unless the job marked itself as failed.
func (i *Interactions) AssertFailed(t testing.TB) {
	t.Helper()
	if ok, _ := i.Runtime.HasFailed(); !ok {
		t.Fatal("expected the job to fail")
	}
}

// AssertFailedWith fails unless the job failed with an error matching want: an error (errors.Is)
// or a message substring.
func (i *Interactions) AssertFailedWith(t testing.TB, want any) {
	t.Helper()
	ok, err := i.Runtime.HasFailed()
	if !ok {
		t.Fatal("expected the job to fail")
	}
	switch w := want.(type) {
	case error:
		if !errors.Is(err, w) {
			t.Fatalf("expected failure %v, got %v", w, err)
		}
	case string:
		if !strings.Contains(err.Error(), w) {
			t.Fatalf("expected failure containing %q, got %v", w, err)
		}
	}
}

// AssertNotFailed fails when the job marked itself as failed.
func (i *Interactions) AssertNotFailed(t testing.TB) {
	t.Helper()
	if ok, err := i.Runtime.HasFailed(); ok {
		t.Fatalf("expected the job not to fail, it failed with %v", err)
	}
}

// AssertHasChain fails unless the job's remaining chain is made of jobs of the given types
// (after PrependToChain / AppendToChain).
func (i *Interactions) AssertHasChain(t testing.TB, jobs ...perago.Job) {
	t.Helper()
	chain := i.Runtime.Chain()
	if len(chain) != len(jobs) {
		t.Fatalf("expected a chain of %d jobs, got %d", len(jobs), len(chain))
	}
	for k, j := range jobs {
		if chain[k].Job != perago.NameOf(j) {
			t.Fatalf("chain[%d] is %s, want %s", k, chain[k].Job, perago.NameOf(j))
		}
	}
}

// AssertDoesntHaveChain fails when the job has jobs chained after it.
func (i *Interactions) AssertDoesntHaveChain(t testing.TB) {
	t.Helper()
	if n := len(i.Runtime.Chain()); n > 0 {
		t.Fatalf("expected no chain, got %d jobs", n)
	}
}

// WithFakeBatch gives job a fake batch (withFakeBatch) and returns what the job does to it.
func WithFakeBatch(job perago.Job) (*Interactions, *perago.Batch, *perago.BatchRecorder) {
	i := WithFakeQueueInteractions(job)
	b, rec := perago.NewFakeBatch(perago.NewUUID(), "")
	i.Runtime.SetFakeBatch(b)
	return i, b, rec
}

// Bus::fake names for the same assertions.

// AssertDispatched is AssertPushed.
func AssertDispatched[T perago.Job](t testing.TB, f *FakeQueue, filters ...func(T) bool) {
	t.Helper()
	AssertPushed(t, f, filters...)
}

// AssertDispatchedTimes is AssertPushedTimes.
func AssertDispatchedTimes[T perago.Job](t testing.TB, f *FakeQueue, n int, filters ...func(T) bool) {
	t.Helper()
	AssertPushedTimes(t, f, n, filters...)
}

// AssertNotDispatched is AssertNotPushed.
func AssertNotDispatched[T perago.Job](t testing.TB, f *FakeQueue, filters ...func(T) bool) {
	t.Helper()
	AssertNotPushed(t, f, filters...)
}

// AssertNothingDispatched is AssertNothingPushed.
func AssertNothingDispatched(t testing.TB, f *FakeQueue) {
	t.Helper()
	AssertNothingPushed(t, f)
}

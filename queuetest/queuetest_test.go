package queuetest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/queuetest"
)

var ctx = context.Background()

type Ship struct {
	perago.Queueable
	OrderID int
}

func (j *Ship) Handle(context.Context) error { return nil }

type Notify struct{ perago.Queueable }

func (j *Notify) Handle(context.Context) error { return nil }

func manager(t *testing.T) (*perago.Manager, *perago.MemoryDriver) {
	q := perago.NewMemoryDriver(0)
	m, err := perago.New(perago.Config{Default: "mem", Connections: map[string]perago.Connection{"mem": {Driver: q}}, Batches: perago.NewMemoryBatchStore()})
	if err != nil {
		t.Fatal(err)
	}
	return m, q
}

func TestFakeAssertions(t *testing.T) {
	m, q := manager(t)
	f := queuetest.Fake(m)
	defer f.Restore()
	queuetest.AssertNothingPushed(t, f)
	_, _ = m.Dispatch(&Ship{OrderID: 7}).OnQueue("shipping").Delay(time.Minute).Send(ctx)
	_ = m.DispatchSync(ctx, &Notify{})
	_ = m.DispatchAfterResponse(ctx, &Notify{})
	_, _ = m.Dispatch(perago.Closure(func(context.Context) error { return nil })).Send(ctx)

	queuetest.AssertPushed(t, f, func(j *Ship) bool { return j.OrderID == 7 })
	queuetest.AssertPushedOn[*Ship](t, f, "shipping")
	queuetest.AssertPushedOnce[*Ship](t, f)
	queuetest.AssertPushedTimes[*Notify](t, f, 2)
	queuetest.AssertDispatchedSync[*Notify](t, f)
	queuetest.AssertDispatchedAfterResponse[*Notify](t, f)
	queuetest.AssertClosurePushed(t, f)
	queuetest.AssertCount(t, f, 4)
	queuetest.AssertNotPushed(t, f, func(j *Ship) bool { return j.OrderID == 8 })
	queuetest.AssertDispatchedWithoutChain[*Ship](t, f)
	if n, _ := q.Size(ctx, "shipping"); n != 0 {
		t.Fatal("faked job reached the driver")
	}
	f.Restore()
	_, _ = m.Dispatch(&Ship{}).Send(ctx)
	if n, _ := q.Size(ctx, "default"); n != 1 {
		t.Fatal("Restore did not remove the fake")
	}
}

func TestFakeForAndExcept(t *testing.T) {
	m, q := manager(t)
	f := queuetest.FakeFor(m, &Ship{})
	_, _ = m.Dispatch(&Ship{}).Send(ctx)
	_, _ = m.Dispatch(&Notify{}).Send(ctx)
	queuetest.AssertPushed[*Ship](t, f)
	queuetest.AssertNotPushed[*Notify](t, f)
	if n, _ := q.Size(ctx, "default"); n != 1 {
		t.Fatal("unfaked job should be queued")
	}
	f.Restore()
	g := queuetest.FakeExcept(m, &Ship{})
	defer g.Restore()
	_, _ = m.Dispatch(&Ship{}).Send(ctx)
	_, _ = m.Dispatch(&Notify{}).Send(ctx)
	queuetest.AssertPushed[*Notify](t, g)
	queuetest.AssertNotPushed[*Ship](t, g)
}

func TestChainsAndBatches(t *testing.T) {
	m, _ := manager(t)
	f := queuetest.Fake(m)
	defer f.Restore()
	_, _ = m.Chain(&Ship{OrderID: 1}, &Notify{}, m.Batch(&Notify{}, &Notify{}).Name("fanout")).Dispatch(ctx)
	queuetest.AssertChained(t, f, &Ship{}, func(n *Notify) bool { return true }, queuetest.ChainedBatch(func(b queuetest.Batched) bool {
		return b.Name == "fanout" && len(b.Jobs) == 2
	}))

	queuetest.AssertNothingBatched(t, f)
	b, err := m.Batch(&Ship{}, []perago.Job{&Notify{}, &Notify{}}).Name("import").AllowFailures().Dispatch(ctx)
	if err != nil || b == nil {
		t.Fatal(err)
	}
	queuetest.AssertBatchCount(t, f, 1)
	queuetest.AssertBatched(t, f, func(b queuetest.Batched) bool {
		return b.Name == "import" && b.AllowFailures && b.HasJobs(&Ship{}, []perago.Job{&Notify{}, &Notify{}})
	})
}

type Worker struct {
	perago.Queueable
	Mode string
}

func (j *Worker) Handle(ctx context.Context) error {
	switch j.Mode {
	case "release":
		j.Release(30 * time.Second)
	case "delete":
		j.Delete()
	case "fail":
		j.Fail(errBroken)
	case "chain":
		return j.AppendToChain(&Notify{})
	case "batch":
		b, _ := j.Batch(ctx)
		if j.Attempts() > 2 {
			return b.Cancel(ctx)
		}
		return b.Add(ctx, &Notify{})
	}
	return nil
}

var errBroken = errors.New("broken")

func TestJobInteractions(t *testing.T) {
	r := queuetest.WithFakeQueueInteractions(&Worker{Mode: "release"})
	_ = r.Handle(ctx)
	r.AssertReleased(t, 30*time.Second)
	r.AssertNotDeleted(t)
	r.AssertNotFailed(t)

	d := queuetest.WithFakeQueueInteractions(&Worker{Mode: "delete"})
	_ = d.Handle(ctx)
	d.AssertDeleted(t)
	d.AssertNotReleased(t)

	f := queuetest.WithFakeQueueInteractions(&Worker{Mode: "fail"})
	_ = f.Handle(ctx)
	f.AssertFailed(t)
	f.AssertFailedWith(t, errBroken)
	f.AssertFailedWith(t, "broken")

	c := queuetest.WithFakeQueueInteractions(&Worker{Mode: "chain"})
	c.AssertDoesntHaveChain(t)
	_ = c.Handle(ctx)
	c.AssertHasChain(t, &Notify{})

	bi, _, rec := queuetest.WithFakeBatch(&Worker{Mode: "batch"})
	_ = bi.Handle(ctx)
	if len(rec.Added) != 1 || rec.Cancelled {
		t.Fatalf("batch recorder = %+v", rec)
	}
	cancel, _, rec2 := queuetest.WithFakeBatch(&Worker{Mode: "batch"})
	cancel.Runtime = perago.NewFakeRuntime(cancel.Runtime.Job(), 3)
	b, _ := perago.NewFakeBatch("x", "")
	cancel.Runtime.SetFakeBatch(b)
	_ = cancel.Handle(ctx)
	_ = rec2
	if !b.Cancelled() {
		t.Fatal("cancel not recorded")
	}
}

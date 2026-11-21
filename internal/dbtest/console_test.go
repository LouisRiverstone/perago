package dbtest_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/console"
	"github.com/LouisRiverstone/perago/database"
)

// Gate fails while gateClosed is set.
type Gate struct {
	perago.Queueable
	N int
}

var (
	gateClosed atomic.Bool
	gatePassed atomic.Int32
)

func (g *Gate) Handle(context.Context) error {
	if gateClosed.Load() {
		return errors.New("gate closed")
	}
	gatePassed.Add(1)
	return nil
}

func init() { perago.Register(&Gate{}) }

type cli struct {
	app      *console.App
	out, err *bytes.Buffer
}

func (c *cli) run(t *testing.T, want int, args ...string) string {
	t.Helper()
	c.out.Reset()
	c.err.Reset()
	if got := c.app.Run(ctx, args); got != want {
		t.Fatalf("%v: exit %d, want %d\nout: %s\nerr: %s", args, got, want, c.out, c.err)
	}
	return c.out.String()
}

// The queue:* commands an operator runs, against a manager whose queue, failed jobs, batches
// and cache all live in the database.
func TestConsoleOnDatabase(t *testing.T) {
	db := setup(t)
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	cache := database.NewCache(db, "")
	cache.SetClock(clk.Now)
	m, err := perago.New(perago.Config{
		Default:     "db",
		Connections: map[string]perago.Connection{"db": {Driver: database.NewDriver(db, database.DriverOptions{Now: clk.Now})}},
		Failed:      database.NewFailedJobStore(db, ""),
		Batches:     database.NewBatchStore(db, ""),
		Cache:       cache,
		Now:         clk.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	c := &cli{out: &bytes.Buffer{}, err: &bytes.Buffer{}}
	c.app = &console.App{
		Manager: func(context.Context) (*perago.Manager, error) { return m, nil },
		Out:     c.out, Err: c.err, Root: t.TempDir(), Env: "testing", Now: clk.Now,
	}
	size := func() int64 {
		n, err := m.Size(ctx, "db", "default")
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	work := []string{"queue:work", "db", "--stop-when-empty", "--sleep=0.001"}

	// Failing jobs end up in failed_jobs, listed by queue:failed.
	gateClosed.Store(true)
	gatePassed.Store(0)
	for i := range 3 {
		if _, err := m.Dispatch(&Gate{N: i}).Send(ctx); err != nil {
			t.Fatal(err)
		}
	}
	c.run(t, 0, work...)
	failed, _ := m.FailedJobs().All(ctx)
	if len(failed) != 3 {
		t.Fatalf("failed jobs = %d", len(failed))
	}
	out := c.run(t, 0, "queue:failed")
	for _, f := range failed {
		if !strings.Contains(out, f.ID) {
			t.Fatalf("queue:failed misses %s:\n%s", f.ID, out)
		}
	}
	if !strings.Contains(out, "Gate") {
		t.Fatalf("queue:failed shows no job name:\n%s", out)
	}

	// queue:forget drops one; queue:retry all puts the rest back.
	c.run(t, 0, "queue:forget", failed[0].ID)
	c.run(t, 1, "queue:forget", "no-such-id")
	gateClosed.Store(false)
	c.run(t, 0, "queue:retry", "all")
	if n := size(); n != 2 {
		t.Fatalf("retried = %d", n)
	}
	if out := c.run(t, 0, "queue:monitor", "db:default", "--max=1"); !strings.Contains(out, "db:default") || !strings.Contains(out, "2") {
		t.Fatalf("queue:monitor:\n%s", out)
	}
	c.run(t, 0, work...)
	if gatePassed.Load() != 2 || size() != 0 {
		t.Fatalf("passed=%d left=%d", gatePassed.Load(), size())
	}
	if out := c.run(t, 0, "queue:failed"); !strings.Contains(out, "No failed jobs") {
		t.Fatalf("queue:failed after retry:\n%s", out)
	}

	// queue:pause holds the queue in the database cache until queue:resume.
	c.run(t, 0, "queue:pause", "db:default")
	_, _ = m.Dispatch(&Gate{N: 9}).Send(ctx)
	c.run(t, 0, work...)
	if size() != 1 {
		t.Fatal("paused queue was processed")
	}
	c.run(t, 0, "queue:resume", "db:default")
	c.run(t, 0, work...)
	if size() != 0 || gatePassed.Load() != 3 {
		t.Fatalf("after resume: left=%d passed=%d", size(), gatePassed.Load())
	}

	// queue:clear, queue:flush and queue:prune-batches clean up.
	_, _ = m.Dispatch(&Gate{}).Send(ctx)
	_, _ = m.Dispatch(&Gate{}).Send(ctx)
	c.run(t, 0, "queue:clear", "db")
	if size() != 0 {
		t.Fatal("queue:clear left jobs")
	}
	gateClosed.Store(true)
	_, _ = m.Dispatch(&Gate{}).Send(ctx)
	c.run(t, 0, work...)
	c.run(t, 0, "queue:flush")
	if left, _ := m.FailedJobs().All(ctx); len(left) != 0 {
		t.Fatalf("queue:flush left %d", len(left))
	}
	gateClosed.Store(false)
	b, err := m.Batch(&Gate{}).Dispatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c.run(t, 0, work...)
	clk.Advance(48 * time.Hour)
	c.run(t, 0, "queue:prune-batches")
	if _, err := m.FindBatch(ctx, b.ID); !perago.IsCode(err, perago.BatchNotFound) {
		t.Fatalf("finished batch not pruned: %v", err)
	}
}

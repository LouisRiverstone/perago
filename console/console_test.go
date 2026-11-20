package console_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/console"
)

var ctx = context.Background()

type Hello struct{ Fail bool }

func (h *Hello) Handle(context.Context) error {
	if h.Fail {
		return errors.New("nope")
	}
	return nil
}

func init() { perago.Register(&Hello{}) }

type fixture struct {
	app *console.App
	m   *perago.Manager
	q   *perago.MemoryDriver
	out *bytes.Buffer
	err *bytes.Buffer
}

func newFixture(t *testing.T) *fixture {
	q := perago.NewMemoryDriver(0)
	m, err := perago.New(perago.Config{Default: "mem", Connections: map[string]perago.Connection{"mem": {Driver: q}},
		Failed: perago.NewMemoryFailedJobStore(), Batches: perago.NewMemoryBatchStore()})
	if err != nil {
		t.Fatal(err)
	}
	out, errb := &bytes.Buffer{}, &bytes.Buffer{}
	app := &console.App{Manager: func(context.Context) (*perago.Manager, error) { return m, nil }, Out: out, Err: errb, Root: t.TempDir(), Env: "testing",
		Now: func() time.Time { return time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC) }}
	return &fixture{app: app, m: m, q: q, out: out, err: errb}
}

func (f *fixture) run(t *testing.T, want int, args ...string) string {
	t.Helper()
	f.out.Reset()
	f.err.Reset()
	if got := f.app.Run(ctx, args); got != want {
		t.Fatalf("%v: exit %d, want %d\nout: %s\nerr: %s", args, got, want, f.out, f.err)
	}
	return f.out.String()
}

func TestWorkFailedRetryForget(t *testing.T) {
	f := newFixture(t)
	_, _ = f.m.Dispatch(&Hello{}).Send(ctx)
	_, _ = f.m.Dispatch(&Hello{Fail: true}).OnQueue("low").Send(ctx)
	out := f.run(t, 0, "queue:work", "--queue=default,low", "--stop-when-empty", "--sleep=0.01", "--tries=1")
	if !strings.Contains(out, "DONE") || !strings.Contains(out, "FAIL") {
		t.Fatalf("work output:\n%s", out)
	}
	out = f.run(t, 0, "queue:failed")
	if !strings.Contains(out, "console_test.Hello") || !strings.Contains(out, "low") {
		t.Fatalf("failed:\n%s", out)
	}
	ids, _ := f.m.FailedJobs().IDs(ctx, "")
	f.run(t, 0, "queue:retry", "all")
	if n, _ := f.q.Size(ctx, "low"); n != 1 {
		t.Fatal("retry did not requeue")
	}
	f.run(t, 1, "queue:forget", ids[0])
	f.run(t, 0, "queue:work", "--queue=low", "--once")
	f.run(t, 0, "queue:forget", ids[0])
	f.run(t, 0, "queue:flush")
	if out := f.run(t, 0, "queue:failed"); !strings.Contains(out, "No failed jobs") {
		t.Fatal(out)
	}
	f.run(t, 0, "queue:prune-failed", "--hours=1")
	f.run(t, 0, "queue:prune-batches", "--hours=1", "--unfinished=72", "--cancelled=72")
}

func TestMonitorClearPauseRestart(t *testing.T) {
	f := newFixture(t)
	for range 3 {
		_, _ = f.m.Dispatch(&Hello{}).Send(ctx)
	}
	var busy int
	perago.Listen(f.m, func(ctx context.Context, e perago.QueueBusy) { busy++ })
	out := f.run(t, 0, "queue:monitor", "mem:default,mem:other", "--max=2")
	if !strings.Contains(out, "ALERT") || !strings.Contains(out, "mem:other") || busy != 1 {
		t.Fatalf("monitor:\n%s busy=%d", out, busy)
	}
	f.run(t, 0, "queue:pause", "mem:default")
	if p, _ := f.m.IsPaused(ctx, "mem", "default"); !p {
		t.Fatal("not paused")
	}
	f.run(t, 0, "queue:resume", "--all")
	if p, _ := f.m.IsPaused(ctx, "mem", "default"); !p {
		t.Fatal("--all resumed an individually paused queue")
	}
	f.run(t, 0, "queue:continue", "mem:default")
	if p, _ := f.m.IsPaused(ctx, "mem", "default"); p {
		t.Fatal("not resumed")
	}
	f.run(t, 0, "queue:pause", "--all")
	if p, _ := f.m.IsPaused(ctx, "mem", "other"); !p {
		t.Fatal("--all did not pause")
	}
	f.run(t, 0, "queue:resume", "--all")
	f.run(t, 0, "queue:restart")
	f.app.Env = "production"
	f.run(t, 1, "queue:clear")
	f.run(t, 0, "queue:clear", "--force")
	if n, _ := f.q.Size(ctx, "default"); n != 0 {
		t.Fatal("clear")
	}
}

func TestMakeCommands(t *testing.T) {
	f := newFixture(t)
	f.run(t, 0, "make:job", "ProcessPodcast", "--batched")
	f.run(t, 1, "make:job", "ProcessPodcast")
	f.run(t, 2, "make:job", "lowercase")
	f.run(t, 0, "make:job", "SendSMS", "--sync")
	f.run(t, 0, "make:job-middleware", "RateLimit")
	f.run(t, 0, "make:queue-table")
	f.run(t, 0, "queue:failed-table")
	f.run(t, 0, "make:queue-batches-table", "--table=batches")
	f.run(t, 0, "make:cache-table")
	read := func(p string) string {
		b, err := os.ReadFile(filepath.Join(f.app.Root, p))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	if s := read("app/jobs/process_podcast.go"); !strings.Contains(s, "package jobs") || !strings.Contains(s, "perago.Register(&ProcessPodcast{})") || !strings.Contains(s, "Cancelled()") {
		t.Fatal(s)
	}
	if s := read("app/jobs/send_sms.go"); !strings.Contains(s, "perago.Sync") {
		t.Fatal(s)
	}
	if s := read("app/jobs/middleware/rate_limit.go"); !strings.Contains(s, "package middleware") {
		t.Fatal(s)
	}
	if s := read("database/migrations/20261120100000_create_batches_table.go"); !strings.Contains(s, `database.CreateBatchesTable(ctx, s, "batches")`) || !strings.Contains(s, "type CreateBatchesTable struct") {
		t.Fatal(s)
	}
	for _, p := range []string{"20261120100000_create_jobs_table.go", "20261120100000_create_failed_jobs_table.go", "20261120100000_create_cache_table.go"} {
		read("database/migrations/" + p)
	}
}

func TestUsageAndErrors(t *testing.T) {
	f := newFixture(t)
	if out := f.run(t, 0); !strings.Contains(out, "queue:work") {
		t.Fatal(out)
	}
	f.run(t, 2, "queue:nope")
	f.run(t, 2, "queue:work", "--tries=x")
	f.run(t, 2, "queue:monitor")
	if out := f.run(t, 0, "queue:work", "--help"); !strings.Contains(out, "--stop-when-empty") {
		t.Fatal(out)
	}
	f.app.Manager = nil
	f.run(t, 1, "queue:restart")
}

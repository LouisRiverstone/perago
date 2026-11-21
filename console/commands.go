package console

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/database"
)

var workerBools = []string{"once", "stop-when-empty", "force"}

func (a *App) commands() map[string]command {
	list := []command{
		{name: "queue:work", usage: "queue:work [connection] [--queue=high,default] [--name=] [--once] [--stop-when-empty] [--tries=1] [--backoff=0] [--timeout=60] [--sleep=3] [--rest=0] [--memory=128|60%] [--max-jobs=0] [--max-time=0] [--force]",
			summary: "Process jobs on a queue", bools: workerBools, run: work},
		{name: "queue:listen", usage: "queue:listen [connection] [queue:work flags]",
			summary: "Run a fresh worker process for each job (picks up code changes)", bools: workerBools, run: listen},
		{name: "queue:restart", usage: "queue:restart", summary: "Tell workers to exit after their current job", run: restart},
		{name: "queue:pause", usage: "queue:pause <connection:queue> | --all [--ttl=]", summary: "Pause a queue", bools: []string{"all"}, run: pause},
		{name: "queue:resume", usage: "queue:resume <connection:queue> | --all", summary: "Resume a paused queue", bools: []string{"all"}, run: resume},
		{name: "queue:continue", usage: "queue:continue <connection:queue> | --all", summary: "Alias of queue:resume", bools: []string{"all"}, run: resume},
		{name: "queue:clear", usage: "queue:clear [connection] [--queue=] [--force]", summary: "Delete every job of a queue", run: clear},
		{name: "queue:monitor", usage: "queue:monitor <connection:queue,...> [--max=1000]", summary: "Show queue sizes; fire QueueBusy over --max", run: monitor},
		{name: "queue:failed", usage: "queue:failed", summary: "List failed jobs", run: failed},
		{name: "queue:retry", usage: "queue:retry <id...|all> [--queue=]", summary: "Push failed jobs back onto their queue", run: retry},
		{name: "queue:retry-batch", usage: "queue:retry-batch <batch-id...>", summary: "Retry the failed jobs of batches", run: retryBatch},
		{name: "queue:forget", usage: "queue:forget <id>", summary: "Delete a failed job", run: forget},
		{name: "queue:flush", usage: "queue:flush [--hours=]", summary: "Delete failed jobs (older than --hours)", run: flush},
		{name: "queue:prune-failed", usage: "queue:prune-failed [--hours=24]", summary: "Delete failed jobs older than --hours", run: pruneFailed},
		{name: "queue:prune-batches", usage: "queue:prune-batches [--hours=24] [--unfinished=] [--cancelled=]", summary: "Delete old batch records", run: pruneBatches},
		{name: "make:job", usage: "make:job <Name> [--sync] [--batched]", summary: "Create a job", bools: []string{"sync", "batched"}, run: makeJob},
		{name: "make:job-middleware", usage: "make:job-middleware <Name>", summary: "Create a job middleware", run: makeMiddleware},
		{name: "make:queue-table", usage: "make:queue-table [--table=jobs] [--format=migro|sql] [--dialect=postgres]", summary: "Create a migration for the jobs table", run: makeTable(database.JobsTable, "CreateJobsTable")},
		{name: "make:queue-failed-table", usage: "make:queue-failed-table [--table=failed_jobs] [--format=migro|sql] [--dialect=postgres]", summary: "Create a migration for the failed jobs table", run: makeTable(database.FailedTable, "CreateFailedJobsTable")},
		{name: "make:queue-batches-table", usage: "make:queue-batches-table [--table=job_batches] [--format=migro|sql] [--dialect=postgres]", summary: "Create a migration for the job batches table", run: makeTable(database.BatchesTable, "CreateBatchesTable")},
		{name: "make:cache-table", usage: "make:cache-table [--table=cache] [--format=migro|sql] [--dialect=postgres]", summary: "Create a migration for the cache table (locks, limits, signals)", run: makeTable(database.CacheTable, "CreateCacheTable")},
	}
	out := make(map[string]command, len(list)+3)
	for _, c := range list {
		out[c.name] = c
	}
	for alias, target := range map[string]string{"queue:table": "make:queue-table", "queue:failed-table": "make:queue-failed-table", "queue:batches-table": "make:queue-batches-table"} {
		c := out[target]
		c.name, c.summary = alias, "Alias of "+target
		out[alias] = c
	}
	return out
}

func duration(v string, unit time.Duration) (time.Duration, error) {
	if v == "" {
		return 0, nil
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q", v)
	}
	return time.Duration(f * float64(unit)), nil
}

func (c *call) workerOptions() (perago.WorkerOptions, error) {
	o := perago.WorkerOptions{Name: c.flag("name", "default"), Once: c.has("once"), StopWhenEmpty: c.has("stop-when-empty"), Force: c.has("force"), HandleSignals: true, Maintenance: c.app.Maintenance}
	if len(c.args) > 0 {
		o.Connection = c.args[0]
	}
	if q := c.flag("queue", ""); q != "" {
		o.Queues = strings.Split(q, ",")
	}
	var err error
	ints := []struct {
		flag string
		dst  *int
	}{{"max-jobs", &o.MaxJobs}}
	for _, f := range ints {
		if v := c.flag(f.flag, ""); v != "" {
			if *f.dst, err = strconv.Atoi(v); err != nil {
				return o, fmt.Errorf("--%s: %v", f.flag, err)
			}
		}
	}
	if v := c.flag("tries", ""); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return o, fmt.Errorf("--tries: %v", err)
		}
		o.Tries = n
		if n == 0 {
			o.Tries = -1
		}
	}
	durs := []struct {
		flag string
		dst  *time.Duration
	}{{"timeout", &o.Timeout}, {"sleep", &o.Sleep}, {"rest", &o.Rest}, {"max-time", &o.MaxTime}}
	for _, f := range durs {
		if *f.dst, err = duration(c.flag(f.flag, ""), time.Second); err != nil {
			return o, fmt.Errorf("--%s: %v", f.flag, err)
		}
	}
	if c.flag("timeout", "") == "0" {
		o.Timeout = -1
	}
	for _, flag := range []string{"backoff", "delay"} {
		v := c.flag(flag, "")
		if v == "" {
			continue
		}
		for _, part := range strings.Split(v, ",") {
			d, err := duration(part, time.Second)
			if err != nil {
				return o, fmt.Errorf("--%s: %v", flag, err)
			}
			o.Backoff = append(o.Backoff, d)
		}
	}
	if v := c.flag("memory", ""); v != "" {
		if p, ok := strings.CutSuffix(v, "%"); ok {
			if o.MemoryPercent, err = strconv.ParseFloat(p, 64); err != nil {
				return o, fmt.Errorf("--memory: %v", err)
			}
			return o, nil
		}
		if o.MemoryMB, err = strconv.Atoi(v); err != nil {
			return o, fmt.Errorf("--memory: %v", err)
		}
	}
	return o, nil
}

// work runs queue:work, printing one line per job like artisan.
func work(c *call) int {
	o, err := c.workerOptions()
	if err != nil {
		c.errorf("%v", err)
		return ExitUsage
	}
	m, ok := c.manager()
	if !ok {
		return ExitError
	}
	starts := map[string]time.Time{}
	line := func(rt *perago.Runtime, status string) {
		took := ""
		if s, ok := starts[rt.UUID()]; ok && status != "RUNNING" {
			took = " " + c.app.Now().Sub(s).Round(time.Millisecond).String()
			delete(starts, rt.UUID())
		}
		c.printf("%s %s %s%s\n", c.app.Now().Format("2006-01-02 15:04:05"), rt.DisplayName(), status, took)
	}
	offs := []func(){
		perago.Listen(m, func(ctx context.Context, e perago.JobProcessing) {
			starts[e.Job.UUID()] = c.app.Now()
			line(e.Job, "RUNNING")
		}),
		perago.Listen(m, func(ctx context.Context, e perago.JobProcessed) { line(e.Job, "DONE") }),
		perago.Listen(m, func(ctx context.Context, e perago.JobReleasedAfterException) { line(e.Job, "RELEASED") }),
		perago.Listen(m, func(ctx context.Context, e perago.JobFailed) { line(e.Job, "FAIL") }),
	}
	defer func() {
		for _, off := range offs {
			off()
		}
	}()
	conn := o.Connection
	if conn == "" {
		conn = m.DefaultConnection()
	}
	if !o.Once {
		c.printf("Processing jobs from the [%s] connection.\n", conn)
	}
	status, err := m.Work(c.ctx, o)
	if err != nil {
		c.errorf("%v", err)
	}
	return status
}

// listen runs queue:work --once in a fresh process again and again (queue:listen).
func listen(c *call) int {
	if _, err := c.workerOptions(); err != nil {
		c.errorf("%v", err)
		return ExitUsage
	}
	exe := c.app.Executable
	if exe == "" {
		var err error
		if exe, err = os.Executable(); err != nil {
			c.errorf("%v", err)
			return ExitError
		}
	}
	args := append(append([]string(nil), c.app.Args...), "queue:work")
	args = append(args, c.args...)
	for k, v := range c.flags {
		if k == "once" || k == "stop-when-empty" {
			continue
		}
		if v == "" {
			args = append(args, "--"+k)
			continue
		}
		args = append(args, "--"+k+"="+v)
	}
	args = append(args, "--once")
	sleep, _ := duration(c.flag("sleep", "3"), time.Second)
	for c.ctx.Err() == nil {
		cmd := exec.CommandContext(c.ctx, exe, args...)
		cmd.Stdout, cmd.Stderr = c.app.Out, c.app.Err
		err := cmd.Run()
		if c.has("stop-when-empty") && err == nil {
			return ExitOK
		}
		if err != nil && c.ctx.Err() == nil {
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				c.errorf("%v", err)
				return ExitError
			}
		}
		select {
		case <-c.ctx.Done():
		case <-time.After(sleep):
		}
	}
	return ExitOK
}

func restart(c *call) int {
	m, ok := c.manager()
	if !ok {
		return ExitError
	}
	if err := m.Restart(c.ctx); err != nil {
		c.errorf("%v", err)
		return ExitError
	}
	c.printf("Broadcasting queue restart signal.\n")
	return ExitOK
}

func targets(c *call, m *perago.Manager) ([][2]string, bool) {
	var out [][2]string
	if len(c.args) == 0 {
		c.errorf("name a connection:queue or pass --all")
		return nil, false
	}
	for _, a := range c.args {
		conn, queue, ok := strings.Cut(a, ":")
		if !ok {
			conn, queue = "", a
		}
		out = append(out, [2]string{conn, queue})
	}
	return out, true
}

func pause(c *call) int {
	m, ok := c.manager()
	if !ok {
		return ExitError
	}
	ttl, _ := duration(c.flag("ttl", ""), time.Second)
	if c.has("all") {
		if err := m.PauseAll(c.ctx, ttl); err != nil {
			c.errorf("%v", err)
			return ExitError
		}
		c.printf("Job processing on all queues has been paused.\n")
		return ExitOK
	}
	ts, ok := targets(c, m)
	if !ok {
		return ExitUsage
	}
	for _, t := range ts {
		if err := m.Pause(c.ctx, t[0], t[1], ttl); err != nil {
			c.errorf("%v", err)
			return ExitError
		}
		c.printf("Job processing on queue [%s:%s] has been paused.\n", or(t[0], m.DefaultConnection()), t[1])
	}
	return ExitOK
}

func resume(c *call) int {
	m, ok := c.manager()
	if !ok {
		return ExitError
	}
	if c.has("all") {
		if err := m.ResumeAll(c.ctx); err != nil {
			c.errorf("%v", err)
			return ExitError
		}
		c.printf("Job processing on all queues has been resumed.\n")
		return ExitOK
	}
	ts, ok := targets(c, m)
	if !ok {
		return ExitUsage
	}
	for _, t := range ts {
		if err := m.Resume(c.ctx, t[0], t[1]); err != nil {
			c.errorf("%v", err)
			return ExitError
		}
		c.printf("Job processing on queue [%s:%s] has been resumed.\n", or(t[0], m.DefaultConnection()), t[1])
	}
	return ExitOK
}

func or(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func clear(c *call) int {
	if !c.confirm("clear the queue") {
		return ExitError
	}
	m, ok := c.manager()
	if !ok {
		return ExitError
	}
	conn := ""
	if len(c.args) > 0 {
		conn = c.args[0]
	}
	n, err := m.Clear(c.ctx, conn, c.flag("queue", ""))
	if err != nil {
		c.errorf("%v", err)
		return ExitError
	}
	c.printf("Cleared %d jobs from the [%s] queue.\n", n, or(c.flag("queue", ""), "default"))
	return ExitOK
}

func monitor(c *call) int {
	if len(c.args) == 0 {
		c.errorf("usage: queue:monitor <connection:queue,...> [--max=1000]")
		return ExitUsage
	}
	m, ok := c.manager()
	if !ok {
		return ExitError
	}
	max, err := strconv.ParseInt(c.flag("max", "1000"), 10, 64)
	if err != nil {
		c.errorf("--max: %v", err)
		return ExitUsage
	}
	var ts []string
	for _, a := range c.args {
		ts = append(ts, strings.Split(a, ",")...)
	}
	rows, err := m.Monitor(c.ctx, ts, max)
	if err != nil {
		c.errorf("%v", err)
		return ExitError
	}
	w := tabwriter.NewWriter(c.app.Out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "Queue\tSize\tPending\tDelayed\tReserved\tStatus")
	count := func(n int64) string {
		if n < 0 {
			return "-"
		}
		return strconv.FormatInt(n, 10)
	}
	for _, r := range rows {
		status := "OK"
		if r.Busy {
			status = "ALERT"
		}
		_, _ = fmt.Fprintf(w, "%s:%s\t%d\t%s\t%s\t%s\t%s\n", r.Connection, r.Queue, r.Size, count(r.Pending), count(r.Delayed), count(r.Reserved), status)
	}
	_ = w.Flush()
	return ExitOK
}

func failed(c *call) int {
	m, ok := c.manager()
	if !ok {
		return ExitError
	}
	all, err := m.FailedJobs().All(c.ctx)
	if err != nil {
		c.errorf("%v", err)
		return ExitError
	}
	if len(all) == 0 {
		c.printf("No failed jobs found.\n")
		return ExitOK
	}
	w := tabwriter.NewWriter(c.app.Out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "ID\tConnection\tQueue\tJob\tFailed At")
	for _, f := range all {
		name := "?"
		if p, err := perago.DecodePayload(f.Payload); err == nil {
			name = p.DisplayName
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", f.ID, f.Connection, f.Queue, name, f.FailedAt.Format("2006-01-02 15:04:05"))
	}
	_ = w.Flush()
	return ExitOK
}

func retry(c *call) int {
	m, ok := c.manager()
	if !ok {
		return ExitError
	}
	if q := c.flag("queue", ""); q != "" {
		if err := m.RetryFailedQueue(c.ctx, q); err != nil {
			c.errorf("%v", err)
			return ExitError
		}
		c.printf("Retried the failed jobs of queue [%s].\n", q)
		return ExitOK
	}
	ids := c.args
	if len(ids) == 1 && ids[0] == "all" {
		var err error
		if ids, err = m.FailedJobs().IDs(c.ctx, ""); err != nil {
			c.errorf("%v", err)
			return ExitError
		}
	}
	if len(ids) == 0 {
		c.errorf("usage: queue:retry <id...|all> [--queue=]")
		return ExitUsage
	}
	status := ExitOK
	for _, id := range ids {
		if err := m.RetryFailed(c.ctx, id); err != nil {
			c.errorf("%s: %v", id, err)
			status = ExitError
			continue
		}
		c.printf("The failed job [%s] has been pushed back onto the queue.\n", id)
	}
	return status
}

func retryBatch(c *call) int {
	if len(c.args) == 0 {
		c.errorf("usage: queue:retry-batch <batch-id...>")
		return ExitUsage
	}
	m, ok := c.manager()
	if !ok {
		return ExitError
	}
	for _, id := range c.args {
		if err := m.RetryBatch(c.ctx, id); err != nil {
			c.errorf("%s: %v", id, err)
			return ExitError
		}
		c.printf("The failed jobs of batch [%s] have been pushed back onto the queue.\n", id)
	}
	return ExitOK
}

func forget(c *call) int {
	if len(c.args) != 1 {
		c.errorf("usage: queue:forget <id>")
		return ExitUsage
	}
	m, ok := c.manager()
	if !ok {
		return ExitError
	}
	gone, err := m.FailedJobs().Forget(c.ctx, c.args[0])
	if err != nil {
		c.errorf("%v", err)
		return ExitError
	}
	if !gone {
		c.errorf("no failed job matches the given ID")
		return ExitError
	}
	c.printf("Failed job deleted successfully.\n")
	return ExitOK
}

func hours(c *call, flag, def string) (time.Duration, error) {
	v := c.flag(flag, def)
	if v == "" {
		return 0, nil
	}
	h, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("--%s: %v", flag, err)
	}
	return time.Duration(h * float64(time.Hour)), nil
}

func flush(c *call) int {
	m, ok := c.manager()
	if !ok {
		return ExitError
	}
	older, err := hours(c, "hours", "")
	if err != nil {
		c.errorf("%v", err)
		return ExitUsage
	}
	n, err := m.FailedJobs().Flush(c.ctx, older)
	if err != nil {
		c.errorf("%v", err)
		return ExitError
	}
	c.printf("Deleted %d failed jobs.\n", n)
	return ExitOK
}

func pruneFailed(c *call) int {
	m, ok := c.manager()
	if !ok {
		return ExitError
	}
	h, err := hours(c, "hours", "24")
	if err != nil {
		c.errorf("%v", err)
		return ExitUsage
	}
	n, err := m.FailedJobs().Prune(c.ctx, c.app.Now().Add(-h))
	if err != nil {
		c.errorf("%v", err)
		return ExitError
	}
	c.printf("%d entries deleted.\n", n)
	return ExitOK
}

func pruneBatches(c *call) int {
	m, ok := c.manager()
	if !ok {
		return ExitError
	}
	store := m.BatchStore()
	if store == nil {
		c.errorf("job batching is not configured")
		return ExitError
	}
	h, err := hours(c, "hours", "24")
	if err != nil {
		c.errorf("%v", err)
		return ExitUsage
	}
	n, err := store.Prune(c.ctx, c.app.Now().Add(-h))
	if err != nil {
		c.errorf("%v", err)
		return ExitError
	}
	for _, opt := range []struct {
		flag string
		fn   func(context.Context, time.Time) (int64, error)
	}{{"unfinished", store.PruneUnfinished}, {"cancelled", store.PruneCancelled}} {
		if !c.has(opt.flag) {
			continue
		}
		h, err := hours(c, opt.flag, "")
		if err != nil {
			c.errorf("%v", err)
			return ExitUsage
		}
		more, err := opt.fn(c.ctx, c.app.Now().Add(-h))
		if err != nil {
			c.errorf("%v", err)
			return ExitError
		}
		n += more
	}
	c.printf("%d entries deleted.\n", n)
	return ExitOK
}

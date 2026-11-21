---
title: Workers
nav_order: 10
description: "queue:work, priorities, deployment, signals, timeouts, pausing, and Supervisor."
---

# Workers

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## The queue:work command

```
perago queue:work                        # default connection, its default queue
perago queue:work redis --queue=emails   # a connection and a queue
perago queue:work --name=mailer          # a name for logs and supervisors
```

From Go, the same flags are `perago.WorkerOptions`:

```go
status, err := m.Work(ctx, perago.WorkerOptions{
	Connection: "redis", Queues: []string{"high", "default"},
	Tries: 3, Backoff: []time.Duration{10 * time.Second}, Timeout: time.Minute,
	Sleep: 3 * time.Second, MaxJobs: 1000, MaxTime: time.Hour, MemoryMB: 256,
	HandleSignals: true,
})
```

Workers are long-lived processes, and code changes need a restart (see [deployment](#queue-workers-and-deployment)). `queue:listen` starts a fresh process for each job instead: slower, but it picks up new code on its own.

| Flag | Meaning |
|---|---|
| `--queue=high,default` | Queues in priority order |
| `--once` | Process one job, or none, and exit |
| `--stop-when-empty` | Exit once every queue is empty |
| `--max-jobs=1000` | Exit after this many jobs |
| `--max-time=3600` | Exit after this many seconds |
| `--sleep=3` | Seconds to wait when the queues are empty |
| `--rest=0` | Seconds to rest after each job |
| `--tries=1` | Attempts for jobs without `Tries()` (`0` = forever) |
| `--backoff=10,30` | Backoff for jobs without `Backoff()` |
| `--timeout=60` | Seconds a job may run (`0` = no limit) |
| `--memory=128` or `--memory=60%` | Exit (status 12) past this memory |
| `--force` | Work even while `App.Maintenance` reports maintenance mode |

Durations take seconds or Go durations (`90s`, `2m`).

## Running the worker inside your application

A worker does not need a process of its own. `m.Work` blocks until the context is cancelled, so start it in a goroutine of your service: the application dispatches jobs and works them, with nothing but the database (or Redis) next to it.

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

go func() {
	if _, err := m.Work(ctx, perago.WorkerOptions{Queues: []string{"high", "default"}}); err != nil {
		slog.Error("worker stopped", "err", err)
	}
}()

http.ListenAndServe(":8080", handler) // the handlers call m.Dispatch(...).Send(ctx)
```

Start several goroutines for more parallelism, each with its own `m.Work` call. When the context is cancelled, each worker finishes its current job and returns. With more than one process (several instances, or separate `queue:work` processes), give the manager a shared cache: see [Choosing a backend](drivers.md#choosing-a-backend-database-or-redis).

## Queue priorities

`--queue=high,low` takes every job from `high` before looking at `low`.

## Queue workers and deployment

Workers keep your code in memory. After a deploy, run `perago queue:restart`: every worker exits after its current job, and your process manager starts new ones. The restart signal lives in the Manager's `Cache`, so the cache must be shared by every worker, which means database or Redis.

## Reacting to worker signals

With `HandleSignals` (always on for `queue:work`):

- **SIGTERM, SIGINT, SIGQUIT**: finish the current job, then exit. A job that implements `Interrupted(os.Signal)` is told right away, so it can stop early.
- **SIGUSR2**: pause. **SIGCONT**: continue.

## Job expiration and timeouts

`retry_after` is the time a reserved job may run before another worker takes it again: `RetryAfter` for database and Redis, the TTR for Beanstalkd, the visibility timeout for SQS. Keep it **longer than the longest job timeout**, or a slow job runs twice.

The worker `--timeout` (or the job's `Timeout()`) cancels the job's context once it expires. The job counts as failed with `JOB_TIMEOUT`: it is retried with backoff, or failed for good when `FailOnTimeout` is set or its attempts are used up. Go cannot kill a goroutine. A job that ignores its context and outlives `WorkerOptions.Grace` (5s) makes the worker exit with status 1, so the process manager can replace it, much like Laravel's worker killing itself on timeout. Give your HTTP clients and sockets timeouts of their own.

## Pausing and resuming

```
perago queue:pause database:default     # workers finish their job, then take nothing from it
perago queue:pause --all                # every queue
perago queue:continue database:default  # or queue:resume
perago queue:resume --all               # lifts --all; individually paused queues stay paused
```

Workers check for restart and pause signals in the cache before each job. If you don't need these features, skip the checks with `m.WithoutInterruptionPolling()`, or with `WorkerOptions.SkipRestartPolling` and `SkipPausePolling`.

## Maintenance mode

Set `console.App.Maintenance` (or `WorkerOptions.Maintenance`) to a function that reports maintenance mode. Workers wait while it returns true, unless they run with `--force`.

## Resource considerations

Release large resources at the end of each job: close files and image handles, and return pooled connections. `--max-jobs` and `--memory` let a leaky worker restart itself.

## Supervisor

Run workers under a process manager that restarts them: Supervisor, systemd, Kubernetes or Nomad. A Supervisor program looks like this:

```ini
[program:perago-worker]
process_name=%(program_name)s_%(process_num)02d
command=/srv/app/bin/app queue:work redis --sleep=3 --tries=3 --max-time=3600
autostart=true
autorestart=true
stopasgroup=true
killasgroup=true
user=app
numprocs=8
redirect_stderr=true
stdout_logfile=/srv/app/storage/logs/worker.log
stopwaitsecs=3600
```

`stopwaitsecs` must be longer than your longest job, so Supervisor waits for it instead of killing it. Then:

```
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl start "perago-worker:*"
```

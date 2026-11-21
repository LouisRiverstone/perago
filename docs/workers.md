---
title: Running workers
nav_order: 11
description: "queue:work and its flags, workers inside your application, priorities, deployment, signals, expiration and timeouts, pausing, Supervisor and systemd."
---

# Running workers

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## The queue:work command

`queue:work` starts a worker that processes jobs as they are pushed, until it is stopped:

```
perago queue:work
```

The `queue:*` commands run inside your program, which knows your jobs and connections (see [Command line](command-line.md)); `perago queue:work` forwards to it. Keep workers running with a process manager such as [Supervisor](#supervisor-configuration).

It prints a line per job, like `artisan queue:work`:

```
Processing jobs from the [redis] connection.
2026-11-21 10:42:01 jobs.ProcessPodcast RUNNING
2026-11-21 10:42:03 jobs.ProcessPodcast DONE 1.874s
2026-11-21 10:42:03 jobs.SendInvoice RUNNING
2026-11-21 10:42:03 jobs.SendInvoice RELEASED 12ms
```

Errors and other diagnostics go to `Config.Logger` (`log/slog`).

From Go, the flags are `perago.WorkerOptions`, and `m.Work` is the worker:

```go
status, err := m.Work(ctx, perago.WorkerOptions{
	Connection: "redis", Queues: []string{"high", "default"},
	Tries: 3, Backoff: []time.Duration{10 * time.Second}, Timeout: time.Minute,
	Sleep: 3 * time.Second, MaxJobs: 1000, MaxTime: time.Hour, MemoryMB: 256,
	HandleSignals: true,
})
```

{: .note }
Workers are long-lived processes: they load your code once. **After a deploy, restart them**, or they keep running the old code (see [deployment](#queue-workers-and-deployment)). Also keep in mind that package-level state survives from one job to the next.

`queue:listen` starts a fresh worker process for every job instead. It picks up a rebuilt binary without a restart, which is handy in development, but it is much slower than `queue:work`:

```
perago queue:listen
```

### Running multiple queue workers

Run `queue:work` several times to process jobs in parallel: in several terminals, or with Supervisor's `numprocs`. Inside one process, start several `m.Work` goroutines. Every driver hands each job to exactly one worker.

### Specifying the connection and queue

The first argument is the connection. Without it, the worker uses `Config.Default`:

```
perago queue:work redis
```

`--queue` picks the queues of that connection to work, in priority order. Without it, the worker takes the connection's default queue:

```
perago queue:work redis --queue=emails
```

### Processing a specified number of jobs

```
perago queue:work --once           # one job, or none, then exit
perago queue:work --max-jobs=1000  # exit after 1000 jobs
```

Combined with a process manager that restarts the worker, `--max-jobs` releases whatever memory the worker gathered.

### Processing all queued jobs and then exiting

```
perago queue:work --stop-when-empty
```

The worker exits once every queue it works is empty. Use it for batch-style containers that should stop when done.

### Processing jobs for a given number of seconds

```
perago queue:work --max-time=3600  # exit after an hour; durations like 1h also work
```

The worker finishes its current job before it exits.

### Worker sleep duration

When there are jobs, the worker takes them one after the other without pausing. When every queue is empty, it sleeps before it polls again:

```
perago queue:work --sleep=3
```

Redis and Beanstalkd can wait for a job instead of polling (`BlockFor`); SQS long-polls (`WaitTime`). `--rest=1` rests after each job, to go easier on a shared database.

### Maintenance mode and queues

Set `console.App.Maintenance` (or `WorkerOptions.Maintenance`) to a function that tells whether the application is in maintenance mode. While it returns true, workers take no jobs. `--force` makes them work anyway:

```go
app := &console.App{
	Manager:     newQueue,
	Maintenance: func() bool { _, err := os.Stat("storage/down"); return err == nil },
}
```

```
perago queue:work --force
```

### Resource considerations

Your process does not restart between jobs, so release what a job holds before it returns: close files and response bodies, stop tickers and goroutines, return pooled connections. A leak grows with every job.

`--memory` stops the worker once the process uses more than a number of megabytes, or a share of the machine's or container's memory; your process manager then starts a fresh one:

```
perago queue:work --memory=256
perago queue:work --memory=60%
```

The worker exits with status 12 when it hits the limit. Without `--memory` there is no limit.

All the flags:

| Flag | Meaning |
|---|---|
| `--queue=high,default` | Queues in priority order |
| `--name=mailer` | A name for logs |
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
| `--force` | Work even in maintenance mode |

Durations take seconds or Go durations (`90s`, `2m`).

## Running the worker inside your application

A worker does not need a process of its own. `m.Work` blocks until its context is cancelled, so it can run in a goroutine of your service: the application dispatches jobs and works them, with nothing but the database (or Redis) next to it.

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

for range 4 { // four workers
	go func() {
		if _, err := m.Work(ctx, perago.WorkerOptions{Queues: []string{"high", "default"}}); err != nil {
			slog.Error("worker stopped", "err", err)
		}
	}()
}

http.ListenAndServe(":8080", handler) // the handlers call m.Dispatch(...).Send(ctx)
```

When the context is cancelled, each worker finishes its current job and returns. With more than one process (several instances, or separate `queue:work` processes), give the Manager a shared cache: see [Choosing a backend](drivers.md#choosing-a-backend-database-or-redis).

## Queue priorities

Some jobs matter more than others. Push them to their own queue:

```go
m.Dispatch(&SendPasswordReset{UserID: id}).OnQueue("high").Send(ctx)
```

and list the queues in priority order. The worker takes every job from `high` before it looks at `low`:

```
perago queue:work --queue=high,low
```

## Queue workers and deployment

Workers keep your code in memory, so a deploy must restart them. `queue:restart` asks every worker to exit gracefully, after its current job, and your process manager starts new ones with the new binary:

```
perago queue:restart
```

The signal is stored in the Manager's `Cache`, and workers check it before each job. Every worker must therefore share the cache: database or Redis. With the default in-process cache, only workers of the same process see it.

## Reacting to worker signals

With `HandleSignals` (always on for `queue:work`):

- **SIGTERM, SIGINT, SIGQUIT**: finish the current job, then exit.
- **SIGUSR2**: pause, **SIGCONT**: continue.

A long job can be told about a shutdown signal while it runs, to stop early and leave things tidy. Implement `Interrupted(os.Signal)` (Laravel's `Interruptible`):

```go
type ImportProducts struct {
	perago.Queueable
	File string
	stop atomic.Bool
}

func (j *ImportProducts) Interrupted(sig os.Signal) { j.stop.Store(true) }

func (j *ImportProducts) Handle(ctx context.Context) error {
	for row := range rows(j.File) {
		if j.stop.Load() {
			j.Release() // pick it up again after the restart
			return nil
		}
		...
	}
	return nil
}
```

`Interrupted` runs only when a signal arrives while the job is running. It does not replace [timeouts](retries.md#timeout) or `Failed`.

## Job expiration and timeouts

### Job expiration

Each connection has a `retry_after`: how long a reserved job may run before the queue assumes its worker died and hands it to another worker.

| Driver | `retry_after` | Default |
|---|---|---|
| database | `DriverOptions.RetryAfter` | 90s |
| Redis | `DriverOptions.RetryAfter` | 90s |
| Beanstalkd | `Options.TTR` (time to run) | 90s |
| SQS | the queue's visibility timeout, set in AWS | 30s (AWS default) |

Set it to the longest time a job may reasonably take.

### Worker timeouts

`--timeout` (default 60 seconds) is the other side: how long the worker lets a job run before it gives up on it. See [Timeout](retries.md#timeout) for what happens then.

{: .warning }
`--timeout` must be **several seconds shorter than `retry_after`**. Otherwise a job that is still running is handed to a second worker, and it runs twice.

## Pausing and resuming queue workers

To stop processing a queue for a while without stopping the workers, pause it:

```
perago queue:pause database:default     # workers finish their job, then take nothing from it
perago queue:pause --all                # every queue
perago queue:pause redis:emails --ttl=600  # resume by itself after 10 minutes
```

and resume it:

```
perago queue:continue database:default  # or queue:resume
perago queue:resume --all               # lifts --all; individually paused queues stay paused
```

Paused queues keep receiving jobs; they just wait. From Go: `m.Pause`, `m.PauseAll`, `m.Resume`, `m.ResumeAll`, `m.IsPaused`. Pausing fires `QueuePaused` and resuming `QueueResumed`.

### Worker restart and pause signals

Workers read the restart and pause signals from the cache before each job. If you never use `queue:restart` or `queue:pause`, skip those reads:

```go
m.WithoutInterruptionPolling() // both, for every worker of this manager

m.Work(ctx, perago.WorkerOptions{SkipRestartPolling: true}) // Worker::$restartable = false
m.Work(ctx, perago.WorkerOptions{SkipPausePolling: true})   // Worker::$pausable = false
```

Workers that skip them ignore `queue:restart` or `queue:pause`.

## Supervisor configuration

In production, something has to keep your `queue:work` processes running: restart them when they exit after `--max-jobs`, `--memory` or `queue:restart`, and when they crash. Supervisor is a common choice on Linux; systemd and Kubernetes work just as well.

### Installing Supervisor

```
sudo apt-get install supervisor
```

### Configuring Supervisor

Supervisor's programs live in `/etc/supervisor/conf.d`. Create `perago-worker.conf`:

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

`numprocs=8` runs eight workers and restarts any that exits. `command` is your application binary with its console (or `perago queue:work` from the project directory).

{: .warning }
`stopwaitsecs` must be longer than your longest job. Otherwise Supervisor kills the worker before the job finishes.

### Starting Supervisor

```
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl start "perago-worker:*"
```

### systemd

The same with systemd, as a template unit started once per worker (`systemctl enable --now perago-worker@{1..8}`):

```ini
# /etc/systemd/system/perago-worker@.service
[Unit]
Description=perago worker %i
After=network.target

[Service]
User=app
WorkingDirectory=/srv/app
ExecStart=/srv/app/bin/app queue:work redis --sleep=3 --tries=3 --max-time=3600
Restart=always
KillSignal=SIGTERM
TimeoutStopSec=3600

[Install]
WantedBy=multi-user.target
```

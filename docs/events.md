---
title: Events and monitoring
nav_order: 13
description: "Job events, Before/After/Failing/Looping, every queue event, queue:monitor and QueueBusy, and queue:clear."
---

# Events and monitoring

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Job events

`m.Before` and `m.After` (`Queue::before`, `Queue::after`) run code before and after every job. Register them once, at startup, for logging or metrics:

```go
m.Before(func(ctx context.Context, e perago.JobProcessing) {
	// e.Connection, e.Job (the *perago.Runtime), e.Job.Payload()
	slog.Info("job starting", "job", e.Job.DisplayName(), "uuid", e.Job.UUID(), "connection", e.Connection)
})

m.After(func(ctx context.Context, e perago.JobProcessed) {
	metrics.Inc("jobs.done", e.Job.Name())
})
```

`m.Looping` (`Queue::looping`) runs before the worker tries to fetch a job. Use it to clean up after the previous job, for example to roll back a transaction a job left open. Return false to skip this round:

```go
m.Looping(func(ctx context.Context, e perago.Looping) bool {
	return db.PingContext(ctx) == nil // database down: wait for the next round
})
```

Every other event is a type you subscribe to with `perago.Listen`. For instance `WorkerIdle`, fired when a worker finds no job on its queues:

```go
off := perago.Listen(m, func(ctx context.Context, e perago.WorkerIdle) {
	// e.Connection, e.Queues
})
defer off() // every registration returns its unsubscribe function
```

| Event | Fired when | Fields |
|---|---|---|
| `JobQueueing`, `JobQueued` | before and after a job is pushed | `Connection`, `Queue`, `Job`, `Payload`, `Delay` (and `ID` once queued) |
| `JobPopping`, `JobPopped` | before and after the worker pops | `Connection`; `Job` is nil when the queues were empty |
| `JobProcessing` | a job is about to run | `Connection`, `Job` |
| `JobProcessed` | a job succeeded | `Connection`, `Job` |
| `JobAttempted` | after every attempt, whatever its outcome | `Connection`, `Job`, `ExceptionOccurred` |
| `JobExceptionOccurred` | `Handle` returned an error or panicked | `Connection`, `Job`, `Err` |
| `JobReleasedAfterException` | the job was released for a retry | `Connection`, `Job`, `Delay` |
| `JobFailed` | the job failed for good | `Connection`, `Job`, `Err` |
| `JobTimedOut` | the job outlived its timeout | `Connection`, `Job` |
| `JobRetryRequested` | `queue:retry` picked a failed job | `Failed` |
| `JobDebounced` | a [debounced](jobs.md#debounced-jobs) job was superseded | `Connection`, `Job` |
| `Looping`, `WorkerIdle` | the worker is about to poll / found nothing | `Connection`, `Queues` |
| `WorkerStarting`, `WorkerStopping` | a worker starts / stops | `Connection`, `Queues`, `Options` / `Status` |
| `QueueBusy` | `queue:monitor` found a queue over its threshold | `Connection`, `Queue`, `Size` |
| `QueuePaused`, `QueueResumed` | `queue:pause`, `queue:resume` | `Connection`, `Queue` (and the pause `TTL`) |
| `QueueFailedOver` | a [failover](dispatching.md#queue-failover) connection was skipped | `Connection`, `Err` |
| `BatchDispatched` | a batch was stored and its jobs queued | `Batch` |

`Job` is the job's `*perago.Runtime`: `Name()`, `DisplayName()`, `UUID()`, `Attempts()`, `Queue()`, `Connection()`, `Payload()`, and `Job()` for the decoded job itself. `go doc github.com/LouisRiverstone/perago` lists the exact fields of each event.

Listeners run synchronously, in the goroutine that fires the event, so keep them fast; hand slow work to a goroutine or another job.

## Monitoring your queues

A sudden flood of jobs can leave a queue behind. `queue:monitor` checks queue sizes and fires `QueueBusy` for every queue holding `--max` jobs or more (1000 by default):

```
perago queue:monitor redis:default,redis:deployments --max=100
```

It prints each queue's size, with the pending, delayed and reserved breakdown when the driver reports it. Run it every minute from cron, a Kubernetes CronJob or a ticker in your application, and react to `QueueBusy`:

```go
perago.Listen(m, func(ctx context.Context, e perago.QueueBusy) {
	alerts.Send(ctx, "queue %s:%s has %d jobs waiting", e.Connection, e.Queue, e.Size)
})
```

From Go, without the command:

```go
statuses, err := m.Monitor(ctx, []string{"redis:default", "redis:deployments"}, 100)
for _, s := range statuses {
	fmt.Println(s.Connection, s.Queue, s.Size, s.Busy)
}

n, err := m.Size(ctx, "redis", "default")
```

## Clearing jobs from queues

To delete every job of a queue:

```
perago queue:clear                      # the default connection's default queue
perago queue:clear redis --queue=emails
```

When the environment is `production`, `queue:clear` asks for confirmation, or needs `--force` when it runs unattended. From Go: `m.Clear(ctx, connection, queue)`, which returns how many jobs were deleted.

| Driver | Clears |
|---|---|
| database, Redis, memory | the ready, delayed and reserved jobs of the queue |
| SQS | purges the queue; AWS allows one purge a minute, and messages sent during the next 60 seconds may be deleted too |
| Beanstalkd | the ready, delayed and buried jobs of the tube |

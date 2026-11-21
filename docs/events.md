---
title: Events and monitoring
nav_order: 12
description: "Queue events, Before/After/Failing/Looping, queue:monitor and queue:clear."
---

# Events and monitoring

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Job events

Subscribe to any event type with `perago.Listen`. It returns the unsubscribe function:

```go
off := perago.Listen(m, func(ctx context.Context, e perago.JobProcessed) {
	metrics.Inc("jobs.done", e.Job.Name())
})

m.Before(func(ctx context.Context, e perago.JobProcessing) { ... })  // Queue::before
m.After(func(ctx context.Context, e perago.JobProcessed) { ... })    // Queue::after
m.Failing(func(ctx context.Context, e perago.JobFailed) { ... })     // Queue::failing
m.Looping(func(ctx context.Context, e perago.Looping) bool {         // Queue::looping
	return db.Ping(ctx) == nil // false skips this iteration
})
```

| Event | When |
|---|---|
| `JobQueueing`, `JobQueued` | Before and after a job is pushed |
| `JobPopping`, `JobPopped` | Before and after the worker pops |
| `JobProcessing`, `JobProcessed` | Before a job runs, after it succeeds |
| `JobAttempted` | After every attempt |
| `JobExceptionOccurred` | `Handle` returned an error or panicked |
| `JobReleasedAfterException` | The job was released for a retry |
| `JobFailed` | The job failed for good |
| `JobTimedOut` | The job outlived its timeout |
| `JobRetryRequested` | `queue:retry` picked a failed job |
| `JobDebounced` | A debounced job was superseded |
| `Looping`, `WorkerStarting`, `WorkerStopping`, `WorkerIdle` | Worker lifecycle |
| `QueueBusy` | `queue:monitor` found a queue over its threshold |
| `QueuePaused`, `QueueResumed` | `queue:pause`, `queue:resume` |
| `QueueFailedOver` | A failover connection was skipped |
| `BatchDispatched` | A batch was stored and its jobs queued |

`e.Job` is the job's `*perago.Runtime`: `Name()`, `DisplayName()`, `UUID()`, `Attempts()`, `Queue()`, `Connection()`, `Payload()`.

## Monitoring your queues

Schedule `queue:monitor` every minute (cron, Kubernetes CronJob, or a ticker):

```
perago queue:monitor redis:default,redis:deployments --max=100
```

It prints each queue's size (with the pending, delayed and reserved breakdown when the driver reports it) and fires `QueueBusy` for every queue holding `--max` jobs or more:

```go
perago.Listen(m, func(ctx context.Context, e perago.QueueBusy) {
	alert.Send(ctx, "queue %s:%s has %d jobs", e.Connection, e.Queue, e.Size)
})
```

From Go: `statuses, err := m.Monitor(ctx, []string{"redis:default"}, 100)`.

## Clearing jobs from queues

```
perago queue:clear                     # the default connection's default queue
perago queue:clear redis --queue=emails
```

In production, `queue:clear` asks for confirmation unless you pass `--force`. On SQS it purges the queue, which AWS allows once a minute. On Beanstalkd it deletes the ready, delayed and buried jobs. From Go: `m.Clear(ctx, connection, queue)`.

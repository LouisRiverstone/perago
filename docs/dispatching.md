---
title: Dispatching
nav_order: 6
description: "Delays, sync and after-response dispatch, bulk, after-commit transactions, routing, failover and closures."
---

# Dispatching

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

```go
id, err := m.Dispatch(&ProcessPodcast{PodcastID: 1}).Send(ctx)
m.DispatchIf(ctx, podcast.Ready, &ProcessPodcast{})
m.DispatchUnless(ctx, podcast.Draft, &ProcessPodcast{})
```

`Send` returns the job's UUID. An empty id with a nil error means the dispatch was skipped on purpose: a false condition, `PrepareForDispatch` returning false, or a unique job whose lock is held.

## Delays

```go
m.Dispatch(job).Delay(10 * time.Minute).Send(ctx)
m.Dispatch(job).DelayUntil(time.Now().Add(time.Hour)).Send(ctx)
j.DelayFor(time.Minute)                 // a default, set on the job
m.Dispatch(j).WithoutDelay().Send(ctx)  // ignore the default
```

## Synchronous and after-response dispatch

```go
err := m.DispatchSync(ctx, job)          // run now, here; returns the job's error
m.DispatchAfterResponse(ctx, job)        // run after the current request (see the deferred driver)
m.Dispatch(job).OnConnection("background").Send(ctx) // run in a goroutine
```

## Bulk dispatching

```go
err := m.Bulk(ctx, &Import{Part: 1}, &Import{Part: 2}, &Import{Part: 3})
```

Jobs are grouped by connection and queue and pushed in one round trip where the driver supports it. Bulk dispatch has none of a batch's tracking or callbacks.

## Preparing jobs before dispatch

```go
func (j *Export) PrepareForDispatch(ctx context.Context) bool {
	j.RequestedAt = time.Now()
	return j.UserID != 0 // false cancels the dispatch
}
```

## Jobs and database transactions

A job dispatched inside a transaction can run before the transaction commits, and see data that is not there yet. Hold it until the commit:

```go
// every dispatch on this connection waits (after_commit)
"database": {Driver: d, AfterCommit: true},

err := database.Transaction(ctx, m, db, func(ctx context.Context, tx *migro.DB) error {
	order, err := orders.CreateWith(ctx, tx, input)
	if err != nil {
		return err // rolled back: the job below is dropped
	}
	_, err = m.Dispatch(&SendReceipt{OrderID: order.ID}).Send(ctx)
	return err
})
```

`m.Dispatch(job).AfterCommit()` and `.BeforeCommit()` decide per dispatch. `j.AfterCommit()`, `j.BeforeCommit()` and embedding `perago.QueueAfterCommit` decide per job. `m.Transaction(ctx, fn)` opens the after-commit scope without a database. `perago.BeginTransaction` gives you `Commit` and `Rollback` to call yourself. Scopes nest.

## Choosing the queue and connection

```go
m.Dispatch(job).OnQueue("processing").OnConnection("sqs").Send(ctx)

func NewProcessPodcast(id int) *ProcessPodcast {
	j := &ProcessPodcast{PodcastID: id}
	j.OnQueue("podcasts")          // a default the dispatch can override
	return j
}
```

### Routing

`Queue::route` sends every job of a type, or every job matching an interface, to a connection and queue unless the dispatch says otherwise:

```go
m.Route(&ProcessPodcast{}, "redis", "podcasts")
perago.RouteInterface[Billing](m, "sqs", "billing")
m.RouteMatching(func(j perago.Job) bool { ... }, "", "low")
```

### Forwarding

`Queue::forward` redirects a queue:

```go
m.Forward("reports", "reports.fifo", "sqs") // an explicit job connection still wins
```

## SQS message groups

```go
m.Dispatch(job).OnGroup("customer-" + id).WithDeduplicationID(key).Send(ctx)
```

A job can also define `MessageGroup()` and `DeduplicationID()` methods. See [SQS](drivers.md#amazon-sqs).

## Queueing closures

A Go closure cannot be serialized. `perago.Closure` runs on in-process connections: sync, deferred, background and memory.

```go
m.Dispatch(perago.Closure(func(ctx context.Context) error { return podcast.Publish(ctx) })).
	OnConnection("background").
	Catch(func(ctx context.Context, err error) { log.Println(err) }).
	Send(ctx)
```

For closures that a worker in another process must run, register them by name:

```go
perago.RegisterClosure("podcasts.publish", func(ctx context.Context, args json.RawMessage) error { ... })
m.Dispatch(perago.Call("podcasts.publish", map[string]int{"id": 1})).Send(ctx)
```

## Error handling

When `Handle` returns an error or panics, the job is released with its backoff until its attempts run out. It then fails: the job's `Failed` method runs, chain and dispatch `Catch` callbacks run, the job is stored in `failed_jobs`, and `JobFailed` fires. See [Jobs](jobs.md#interacting-with-the-queue) for manual `Release`, `Fail` and `Delete`.

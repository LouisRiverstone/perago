---
title: Dispatching jobs
nav_order: 7
description: "Dispatch, delays, sync and deferred dispatch, bulk, preparing jobs, transactions, queues and connections, routing, SQS FIFO, failover and closures."
---

# Dispatching jobs

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

Laravel's `ProcessPodcast::dispatch($podcast)` passes its arguments to the constructor. In perago you build the struct and hand it to the Manager:

```go
id, err := m.Dispatch(&ProcessPodcast{PodcastID: podcast.ID}).Send(ctx)
```

`Dispatch` returns a `*PendingDispatch` that you configure (delay, queue, connection...). Nothing is queued until `Send`. `Send` returns the job's UUID, or an empty id with a nil error when the dispatch was skipped on purpose: a false condition, `PrepareForDispatch` returning false, or a [unique job](jobs.md#unique-jobs) whose lock is held.

To dispatch conditionally (`dispatchIf`, `dispatchUnless`):

```go
m.DispatchIf(ctx, account.Active, &ProcessPodcast{PodcastID: id})
m.DispatchUnless(ctx, account.Suspended, &ProcessPodcast{PodcastID: id})
m.Dispatch(job).If(account.Active).OnQueue("podcasts").Send(ctx) // in the middle of a chain of calls
```

The job goes to `Config.Default`. When that is empty, the `PERAGO_CONNECTION` or `QUEUE_CONNECTION` environment variable picks the connection, and `sync` is the last resort.

## Delayed dispatching

A delayed job is invisible to workers until its time comes:

```go
m.Dispatch(job).Delay(10 * time.Minute).Send(ctx)
m.Dispatch(job).DelayUntil(time.Now().Add(time.Hour)).Send(ctx)
```

A job can carry a default delay, set in its constructor; a dispatch can ignore it with `WithoutDelay`:

```go
func NewProcessPodcast(id int64) *ProcessPodcast {
	j := &ProcessPodcast{PodcastID: id}
	j.DelayFor(time.Minute)
	return j
}

m.Dispatch(NewProcessPodcast(id)).WithoutDelay().Send(ctx)
```

{: .note }
SQS limits delays to 15 minutes. The SQS driver works around it: a job with a longer delay is sent again with the remaining delay until it is due. FIFO queues accept no per-message delay at all.

## Synchronous dispatching

`DispatchSync` runs the job right away, in the current goroutine, and returns its error. The job's middleware, `Failed` hook and events run as usual, but nothing is queued, and a failure is not written to `failed_jobs`: you get the error immediately.

```go
if err := m.DispatchSync(ctx, &ProcessPodcast{PodcastID: id}); err != nil {
	return err
}
```

A job that embeds `perago.Sync` (`make:job --sync`) always runs this way, whatever its connection. A connection whose driver is `perago.SyncDriver()` does the same for every job sent to it, which is handy in local development and tests.

### Deferred dispatching

The `deferred` driver runs jobs in the current process **after the HTTP response has been sent**, so the user does not wait for them and no worker is needed. Wrap your router with `m.Middleware`:

```go
"deferred": {Driver: perago.DeferredDriver()},

http.ListenAndServe(":8080", m.Middleware(router))

// in a handler
m.Dispatch(&RecordVisit{Path: r.URL.Path}).OnConnection("deferred").Send(r.Context())
m.DispatchAfterResponse(r.Context(), &RecordVisit{Path: r.URL.Path}) // same thing
```

Outside an HTTP request, open the scope yourself with `ctx, flush := perago.WithDeferred(ctx)` and call `flush()` when the work is done. Without a scope, a deferred job starts right away in a goroutine.

The `background` driver runs each job in a goroutine of its own, without waiting for the response. `m.Wait()` and `m.Close()` wait for those goroutines, so call one of them before your program exits.

Both run in this process only: if it crashes, the job is lost. Use a real queue for work that must happen.

## Bulk dispatching

`Bus::bulk` pushes many independent jobs in as few round trips as possible:

```go
jobs := make([]perago.Job, 0, len(users))
for _, u := range users {
	jobs = append(jobs, &ProcessUser{UserID: u.ID})
}
err := m.Bulk(ctx, jobs...)
```

Jobs are grouped by connection and queue, and each group is pushed in one call when the driver supports it (`BulkPusher`: database, Redis, SQS). Bulk dispatch has none of a [batch](batches.md)'s tracking or callbacks.

## Preparing jobs before dispatch

A job with a `PrepareForDispatch(ctx) bool` method (`PreparesForDispatch`) runs it just before it is queued. Fill in fields, or return false to cancel the dispatch:

```go
func (j *SyncPodcast) PrepareForDispatch(ctx context.Context) bool {
	syncing, _, _ := cache.Get(ctx, "podcast-syncing:"+strconv.FormatInt(j.PodcastID, 10))
	return syncing == ""
}
```

## Jobs and database transactions

A job dispatched inside a database transaction can be picked up by a worker before the transaction commits. The worker then cannot see the rows the transaction created, or, if the transaction rolls back, works on data that never existed.

Set `AfterCommit` on the connection (Laravel's `after_commit`) and run the transaction through perago, so dispatches inside it wait for the commit:

```go
"database": {Driver: d, AfterCommit: true},

err := database.Transaction(ctx, m, sqlDB, func(ctx context.Context, tx *sql.Tx) error {
	order, err := orders.CreateWith(ctx, tx, input)
	if err != nil {
		return err // rolled back: the job below is dropped
	}
	_, err = m.Dispatch(&SendReceipt{OrderID: order.ID}).Send(ctx)
	return err
})
```

The job is pushed once the transaction commits, and discarded if it rolls back. A dispatch outside any transaction is pushed at once. On migro, `peragomigro.Transaction` does the same with a migro transaction; see [Using with migro](migro.md#dispatching-after-commit).

Other ways to open the scope:

- `m.Transaction(ctx, fn)` opens it without a database, around any code;
- `perago.BeginTransaction(ctx)` returns a context and a `*Tx` whose `Commit` and `Rollback` you call yourself, to plug in another transaction manager.

Scopes nest: jobs wait for the outermost one.

### Specifying commit dispatch behavior inline

On a connection without `AfterCommit`, a single dispatch can still wait for the commit, and on a connection with it, a single dispatch can skip the wait:

```go
m.Dispatch(job).AfterCommit().Send(ctx)
m.Dispatch(job).BeforeCommit().Send(ctx)
```

A job can make the choice for every dispatch of its type by embedding `perago.QueueAfterCommit` (`ShouldQueueAfterCommit`), or by calling `j.AfterCommit()` / `j.BeforeCommit()` in its constructor.

## Job chaining

A chain runs jobs one after another, each only if the previous one succeeded:

```go
m.Chain(&ProcessPodcast{ID: 1}, &OptimizePodcast{ID: 1}, &ReleasePodcast{ID: 1}).Dispatch(ctx)
```

[Chains](chains.md) covers their connection and queue, adding jobs from inside a chain, and failures.

## Customizing the queue and connection

### Dispatching to a particular queue

Queues split the jobs of one connection, so you can give some of them more workers, or a higher priority:

```go
m.Dispatch(&ProcessPodcast{PodcastID: id}).OnQueue("processing").Send(ctx)
```

A job can choose its own queue in its constructor; a dispatch can still override it:

```go
func NewProcessPodcast(id int64) *ProcessPodcast {
	j := &ProcessPodcast{PodcastID: id}
	j.OnQueue("processing")
	return j
}
```

`OnQueue` changes the queue, never the connection.

### Dispatching to a particular connection

```go
m.Dispatch(job).OnConnection("sqs").Send(ctx)
m.Dispatch(job).OnConnection("sqs").OnQueue("processing").Send(ctx)

j.OnConnection("sqs") // in the job's constructor
```

### Queue routing

`Queue::route` sends every job of a type to a connection and queue, so the choice is made once instead of at every dispatch:

```go
m.Route(&ProcessPodcast{}, "redis", "podcasts")
m.Route(&ProcessVideo{}, "", "videos") // the default connection, the "videos" queue
m.Route(&SendReport{}, "sqs", "")      // the connection's default queue
```

Laravel can route by interface, trait or parent class. In Go, route by interface or by predicate:

```go
type RequiresVideo interface{ VideoID() int64 }

perago.RouteInterface[RequiresVideo](m, "redis", "video")
m.RouteMatching(func(j perago.Job) bool { return strings.HasPrefix(perago.NameOf(j), "billing.") }, "sqs", "billing")
```

An explicit `OnConnection` or `OnQueue`, on the dispatch or in the job's constructor, wins over a route.

### Queue forwarding

`Queue::forward` moves the traffic of a queue elsewhere, for instance while migrating to another backend, without touching the code that dispatches:

```go
m.Forward("reports", "reports.fifo", "sqs") // "reports" jobs go to sqs, queue "reports.fifo"
m.Forward("payments", "payments", "sqs")   // same queue name, another connection
m.Forward("updates", "notifications", "")  // another queue, same connection
```

A connection the job chose explicitly wins over a forwarded one.

## SQS FIFO and fair queues

SQS FIFO queues (names ending in `.fifo`) deliver messages in order and drop duplicates. A **message group** keeps order inside the group: jobs in the same group run one at a time, jobs in different groups run in parallel.

```go
m.Dispatch(&ProcessOrder{OrderID: o.ID}).
	OnGroup("customer-" + strconv.FormatInt(o.CustomerID, 10)).
	Send(ctx)
```

Without a group, the queue name is the group, so the whole queue runs in order. The **deduplication id** decides which messages are duplicates. It is `WithDeduplicationID(id)` on the dispatch, the job's `DeduplicationID()` method, or the job's UUID:

```go
func (j *RenewSubscription) DeduplicationID() string { return "renewal-" + j.SubscriptionID }
```

### Fair queues

On a **standard** SQS queue, a message group turns on SQS fair queues: one busy tenant cannot hold up the others. No other setting is needed. A job can name its group itself instead of `OnGroup`:

```go
func (j *ProcessOrder) MessageGroup() string { return "customer-" + strconv.FormatInt(j.CustomerID, 10) }
```

Laravel also gives listeners, mail and notifications `messageGroup` and a deduplicator. perago queues jobs only; wrap those in a job to give them a group.

## Queue failover

A failover connection tries a list of connections in order and pushes to the first that accepts the job, so a Redis outage does not lose dispatches:

```go
Default: "failover",
Connections: map[string]perago.Connection{
	"failover": {Driver: perago.FailoverDriver("redis", "database", "sync")},
	"redis":    {Driver: redis.NewDriver(rdb, redis.DriverOptions{})},
	"database": {Driver: database.NewDriver(db, database.DriverOptions{})},
	"sync":     {Driver: perago.SyncDriver()},
},
```

Run a worker for each real connection in the list (`queue:work redis`, `queue:work database`); `sync`, `background` and `deferred` need none. Each connection that fails fires `QueueFailedOver`; when every one fails, the dispatch returns `FAILOVER_EXHAUSTED`.

```go
perago.Listen(m, func(ctx context.Context, e perago.QueueFailedOver) {
	slog.Warn("queue connection down", "connection", e.Connection, "err", e.Err)
})
```

## Queueing closures

Laravel can queue a closure because it serializes the closure's code. Go cannot serialize a function, so perago has two forms.

`perago.Closure` runs a function on an in-process connection (sync, deferred, background, memory), where nothing has to be serialized:

```go
m.Dispatch(perago.Closure(func(ctx context.Context) error {
	return podcast.Publish(ctx)
})).
	OnConnection("background").
	Catch(func(ctx context.Context, err error) {
		slog.Error("publish failed", "err", err) // runs once the closure has failed for good
	}).
	Send(ctx)
```

Sending a `perago.Closure` to a connection served by other processes fails with `CLOSURE_NOT_SERIALIZABLE`. For those, register the function under a name at startup, in every process. The payload then carries the name and the arguments as JSON:

```go
perago.RegisterClosure("podcasts.publish", func(ctx context.Context, args json.RawMessage) error {
	var a struct{ ID int64 }
	if err := json.Unmarshal(args, &a); err != nil {
		return err
	}
	return podcasts.Publish(ctx, a.ID)
})

m.Dispatch(perago.Call("podcasts.publish", map[string]int64{"ID": 1})).Send(ctx)
```

A `Catch` closure runs only in this process. When the closure may fail in another worker, use `CatchJob(&NotifyPublishFailed{})`, a registered job that reads the error with `perago.ErrorFrom(ctx)`.

## Error handling

When `Handle` returns an error or panics, the job is released and retried with its backoff until its attempts run out. It then fails: its `Failed` method runs, `Catch` callbacks run, it is stored in `failed_jobs`, and `JobFailed` fires. [Attempts, timeouts and errors](retries.md) covers how attempts are counted, timeouts, and releasing or failing a job by hand.

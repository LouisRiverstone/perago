---
title: Batches
nav_order: 10
description: "Dispatch groups of jobs, track their progress, react to completion and failure, add jobs, cancel, retry and prune batches."
---

# Batches

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

A batch is a group of jobs that run in parallel and are tracked together: how many are pending, how many failed, and callbacks when the batch completes. Batches need a store for that bookkeeping, `Config.Batches`:

| Store | Constructor |
|---|---|
| Database | `database.NewBatchStore(db, "job_batches")`, table from `perago make:queue-batches-table` (or `database.Install`) |
| DynamoDB | `dynamodb.NewBatchStore(client, opts)`, see [below](#storing-batches-in-dynamodb) |
| Memory | `perago.NewMemoryBatchStore()`, one process only |

```
perago make:queue-batches-table --dialect=postgres
```

Without a store, dispatching a batch fails with `INVALID_CONFIG`.

## Defining batchable jobs

Any job can be batched: `perago.Queueable` already includes Laravel's `Batchable`. A batched job should check whether its batch was cancelled before doing its work:

```go
type ImportCsv struct {
	perago.Queueable
	From, To int
}

func (j *ImportCsv) Handle(ctx context.Context) error {
	b, err := j.Batch(ctx)
	if err != nil {
		return err
	}
	if b.Cancelled() {
		return nil // the batch was cancelled: skip the work
	}
	return importRows(ctx, j.From, j.To)
}
```

`perago make:job ImportCsv --batched` writes that check for you, and the [`SkipIfBatchCancelled`](#cancelling-batches) middleware does it without code. `j.BatchID()` returns the id without loading the batch.

## Dispatching batches

`Bus::batch` is `m.Batch`. Add callbacks, then `Dispatch`:

```go
batch, err := m.Batch(
	&ImportCsv{From: 1, To: 100},
	&ImportCsv{From: 101, To: 200},
	&ImportCsv{From: 201, To: 300},
).
	Before(func(ctx context.Context, b *perago.Batch) {
		// the batch is stored, but no job is queued yet
	}).
	Progress(func(ctx context.Context, b *perago.Batch) {
		// a job completed successfully
	}).
	Then(func(ctx context.Context, b *perago.Batch) {
		// every job completed successfully
	}).
	Catch(func(ctx context.Context, b *perago.Batch, err error) {
		// the first job failure
	}).
	Finally(func(ctx context.Context, b *perago.Batch) {
		// the batch has finished running
	}).
	Dispatch(ctx)

batch.ID // look the batch up later with m.FindBatch
```

Workers run the jobs in parallel, so they can complete in any order. `DispatchIf(ctx, cond)` dispatches only when `cond` is true.

The callbacks run in the worker that completes the batch, so a Go closure runs only when that worker is the process that dispatched the batch (an application running its workers in-process). With workers in other processes, use `ThenJob`, `CatchJob`, `FinallyJob` and `ProgressJob` with registered jobs. They travel with the batch, and read it with `perago.BatchFrom(ctx)` and the error with `perago.ErrorFrom(ctx)`:

```go
type ImportFinished struct {
	perago.Queueable
	UserID int64
}

func (j *ImportFinished) Handle(ctx context.Context) error {
	b := perago.BatchFrom(ctx)
	return notify.User(ctx, j.UserID, "import %s finished: %d rows", b.Name, b.TotalJobs)
}

m.Batch(jobs...).ThenJob(&ImportFinished{UserID: user.ID}).Dispatch(ctx)
```

{: .note }
Unique locks do not apply to jobs inside a batch, as in Laravel.

### Naming batches

A name makes a batch easy to recognise in logs, in `queue:failed` and in your own dashboards:

```go
m.Batch(jobs...).Name("Import CSV").Dispatch(ctx)
```

### Batch connection and queue

Every job of a batch runs on the same connection and queue:

```go
m.Batch(jobs...).OnConnection("redis").OnQueue("imports").Dispatch(ctx)
```

## Chains and batches

A batch can contain chains: pass a `[]perago.Job` (or an `m.Chain(...)`) as an item. The two chains below run in parallel, each one in order:

```go
m.Batch(
	[]perago.Job{&ReleasePodcast{ID: 1}, &SendPodcastReleaseNotification{ID: 1}},
	[]perago.Job{&ReleasePodcast{ID: 2}, &SendPodcastReleaseNotification{ID: 2}},
).Then(func(ctx context.Context, b *perago.Batch) {
	// both chains are done
}).Dispatch(ctx)
```

Every job of every chain counts towards the batch's totals. The reverse, batches inside a chain, is in [Chains](chains.md#batches-inside-chains).

## Adding jobs to batches

Dispatching thousands of jobs from a web request is slow. Dispatch a few loader jobs instead, and let them fill the batch from the workers:

```go
m.Batch(&LoadImportBatch{Page: 1}).
	Then(func(ctx context.Context, b *perago.Batch) { ... }).
	Name("Import Contacts").
	Dispatch(ctx)
```

```go
func (j *LoadImportBatch) Handle(ctx context.Context) error {
	b, err := j.Batch(ctx)
	if err != nil || b.Cancelled() {
		return err
	}
	jobs := make([]any, 0, 1000)
	for i := range 1000 {
		jobs = append(jobs, &ImportContacts{Row: j.Page*1000 + i})
	}
	return b.Add(ctx, jobs...)
}
```

Only a job of the batch can add jobs to it, since the batch is still running then. Added jobs go to the batch's connection and queue.

## Inspecting batches

A `*perago.Batch` tells you where a batch is:

```go
b.ID             // the batch UUID
b.Name           // the name, if any
b.TotalJobs      // jobs assigned to the batch
b.PendingJobs    // jobs not processed yet
b.FailedJobs     // jobs that failed
b.FailedJobIDs   // their UUIDs
b.ProcessedJobs() // jobs processed so far
b.Progress()      // 0 to 100
b.Finished()      // every job has run
b.Cancelled()     // the batch was cancelled
b.HasFailures(); b.HasPendingJobs()
b.CreatedAt; b.FinishedAt; b.CancelledAt

b, err = b.Fresh(ctx) // reload it from the store
```

Look a batch up by id with `m.FindBatch` (`Bus::findBatch`). It returns `BATCH_NOT_FOUND` when the id is unknown:

```go
b, err := m.FindBatch(ctx, id)
if perago.IsCode(err, perago.BatchNotFound) { ... }
```

`m.Batches(ctx, limit, before)` lists the most recent batches, for an admin page.

### Returning batches from routes

`*Batch` marshals to JSON with the same fields as Laravel's `Batch::toArray`, so a progress endpoint is a few lines:

```go
http.HandleFunc("GET /batch/{id}", func(w http.ResponseWriter, r *http.Request) {
	b, err := m.FindBatch(r.Context(), r.PathValue("id"))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	_ = json.NewEncoder(w).Encode(b)
})
```

```json
{"id":"9a1f…","name":"Import CSV","totalJobs":300,"pendingJobs":120,"processedJobs":180,
 "progress":60,"failedJobs":0,"options":{"queue":"imports","allowFailures":false},
 "createdAt":1800000000,"cancelledAt":null,"finishedAt":null}
```

## Cancelling batches

Any code with the batch can cancel it, for instance a job that finds the import is broken:

```go
func (j *ImportCsv) Handle(ctx context.Context) error {
	b, err := j.Batch(ctx)
	if err != nil {
		return err
	}
	if j.tooManyErrors() {
		return b.Cancel(ctx)
	}
	...
}
```

Cancelling does not remove jobs from the queue: they still run, so they should check `Cancelled`, or use `SkipIfBatchCancelled`, which deletes them without running them:

```go
func (j *ImportCsv) Middleware() []perago.Middleware {
	return []perago.Middleware{middleware.SkipIfBatchCancelled()}
}
```

## Batch failures

When a batched job fails for good, the batch's `Catch` callbacks run. They run only for the **first** failure, and that failure marks the batch as cancelled.

### Allowing failures

`AllowFailures` keeps the batch going when jobs fail: it is not cancelled, the other jobs keep running, and `Finally` runs once every job has run. `Then` runs only when no job failed, or after the failed ones were retried successfully.

```go
m.Batch(jobs...).AllowFailures().Dispatch(ctx)
```

Laravel's `allowFailures(fn)` also runs a closure on each failure. In perago, react to each failure with the job's `Failed` method or a `JobFailed` listener (`e.Job.Payload().BatchID` tells you the batch).

### Retrying failed batch jobs

```
perago queue:retry-batch 32dbc76c-4f82-4749-b610-a639fe0099b5
```

or `m.RetryBatch(ctx, id)`. Every failed job of the batch goes back on its queue. When they succeed, the batch finishes and `Then` runs.

## Pruning batches

Batch records pile up. Delete old ones daily, from cron, a Kubernetes CronJob or a ticker in your app:

```
perago queue:prune-batches                              # finished batches older than 24 hours
perago queue:prune-batches --hours=48                   # ... older than 48 hours
perago queue:prune-batches --hours=48 --unfinished=72   # also unfinished ones older than 72 hours
perago queue:prune-batches --hours=48 --cancelled=72    # also cancelled ones older than 72 hours
```

Unfinished batches are those whose jobs never all ran, for example because a job failed and was never retried.

## Storing batches in DynamoDB

The DynamoDB stores live in their own module:

```
go get github.com/LouisRiverstone/perago/dynamodb
```

### DynamoDB batch table configuration

Create the table yourself, usually named `job_batches`, with:

- a string partition key `application`: the `Options.Application` value, so several applications can share a table;
- a string sort key `id`.

To have old batches removed automatically, turn on DynamoDB's TTL on the `ttl` attribute.

### DynamoDB configuration

```go
cfg, _ := config.LoadDefaultConfig(ctx) // AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION...
client := awsdynamodb.NewFromConfig(cfg)

m, err := perago.New(perago.Config{
	// ...
	Batches: dynamodb.NewBatchStore(client, dynamodb.Options{
		Table:       "job_batches",
		Application: "myapp",
		TTL:         7 * 24 * time.Hour,
	}),
})
```

Counter updates are single atomic `UpdateItem` calls, so concurrent workers never lose a count.

### Pruning batches in DynamoDB

Unlike Laravel's DynamoDB repository, perago's store supports `queue:prune-batches`, but it has to scan the application's items to do it. On large tables, prefer DynamoDB's own TTL: set `Options.TTL`, and every item gets a `ttl` attribute that long after its last update, which DynamoDB deletes once TTL is enabled on the table.

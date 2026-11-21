---
title: Batches
nav_order: 9
description: "Dispatch groups of jobs, track their progress, react to completion and failure."
---

# Batches

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

Batches need a `Config.Batches` store: `database.NewBatchStore` (table from `perago make:queue-batches-table`), `dynamodb.NewBatchStore`, or `perago.NewMemoryBatchStore()` for a single process.

## Defining batchable jobs

Any job can be batched. Batched jobs should check whether their batch was cancelled, or use `middleware.SkipIfBatchCancelled()`:

```go
func (j *ImportCsv) Handle(ctx context.Context) error {
	b, err := j.Batch(ctx)
	if err != nil || b.Cancelled() {
		return err
	}
	...
}
```

`perago make:job ImportCsv --batched` writes that check for you.

## Dispatching batches

```go
batch, err := m.Batch(
	&ImportCsv{From: 1, To: 100},
	&ImportCsv{From: 101, To: 200},
	[]perago.Job{&FlushCache{}, &Reindex{}}, // a chain inside the batch
).
	Name("Import CSV").
	OnConnection("redis").OnQueue("imports").
	Before(func(ctx context.Context, b *perago.Batch) { /* stored, nothing queued yet */ }).
	Progress(func(ctx context.Context, b *perago.Batch) { /* a job completed */ }).
	Then(func(ctx context.Context, b *perago.Batch) { /* every job succeeded */ }).
	Catch(func(ctx context.Context, b *perago.Batch, err error) { /* the first failure */ }).
	Finally(func(ctx context.Context, b *perago.Batch) { /* every job has run */ }).
	Dispatch(ctx)

batch.ID
```

The callbacks run inline in the worker that completes the batch. Closures therefore run only when that worker lives in this process. With remote workers, use `ThenJob`, `CatchJob`, `FinallyJob` and `ProgressJob` with registered jobs, which read the batch with `perago.BatchFrom(ctx)` and the error with `perago.ErrorFrom(ctx)`. A chain inside a batch counts each of its jobs. Unique locks do not apply to batched jobs.

## Adding jobs to a batch

```go
func (j *LoadImportBatch) Handle(ctx context.Context) error {
	b, _ := j.Batch(ctx)
	if b.Cancelled() {
		return nil
	}
	return b.Add(ctx, &ImportContacts{Page: 2}, &ImportContacts{Page: 3})
}
```

## Inspecting batches

```go
b, err := m.FindBatch(ctx, id)  // Bus::findBatch; ErrBatchNotFound when missing
b.ID; b.Name; b.TotalJobs; b.PendingJobs; b.FailedJobs; b.FailedJobIDs
b.ProcessedJobs(); b.Progress()  // 0..100
b.Finished(); b.Cancelled(); b.HasFailures(); b.HasPendingJobs()
b, _ = b.Fresh(ctx)
```

`*Batch` marshals to JSON like Laravel's `Batch::toArray`, so an HTTP handler can return it as is:

```go
http.HandleFunc("GET /batch/{id}", func(w http.ResponseWriter, r *http.Request) {
	b, err := m.FindBatch(r.Context(), r.PathValue("id"))
	if err != nil { http.NotFound(w, r); return }
	_ = json.NewEncoder(w).Encode(b)
})
```

## Cancelling batches

`b.Cancel(ctx)` marks the batch cancelled. Jobs that are already queued still run, unless they check `Cancelled` or use `SkipIfBatchCancelled`.

## Batch failures

By default, the first failure cancels the batch and runs the `Catch` callbacks. `AllowFailures()` keeps the batch going. `Finally` still runs once every job has run, and `Then` runs only if every job eventually succeeds.

Retry the failed jobs of a batch with `m.RetryBatch(ctx, id)` or `perago queue:retry-batch <id>`. When they succeed, the batch finishes and `Then` runs.

## Pruning batches

```
perago queue:prune-batches --hours=48                   # finished batches older than 48h
perago queue:prune-batches --hours=48 --unfinished=72   # also unfinished ones older than 72h
perago queue:prune-batches --hours=48 --cancelled=72    # also cancelled ones
```

## Storing batches in DynamoDB

The DynamoDB stores live in their own module: `go get github.com/LouisRiverstone/perago/dynamodb`.

```go
db := awsdb.NewFromConfig(cfg)
Batches: dynamodb.NewBatchStore(db, dynamodb.Options{Table: "job_batches", Application: "myapp", TTL: 30 * 24 * time.Hour}),
```

The table needs a string partition key `application` and a string sort key `id`. Turn on DynamoDB TTL on the `ttl` attribute to expire old batches, which replaces pruning. Counter updates are single atomic `UpdateItem` calls.

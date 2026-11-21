---
title: Failed jobs
nav_order: 12
description: "Storing, inspecting, retrying, forgetting and pruning failed jobs; DynamoDB storage; failed job events."
---

# Dealing with failed jobs

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

Sometimes a job fails for good. A job **fails** when:

- it comes back after its last attempt (`MAX_ATTEMPTS_EXCEEDED`), or after its `RetryUntil` deadline;
- it reaches `MaxExceptions` (`MAX_EXCEPTIONS_EXCEEDED`);
- it times out and has `FailOnTimeout` (`JOB_TIMEOUT`);
- its error matches `DontRetry` or `FailOnException`;
- it calls `Fail`.

It is then deleted from its queue and written to the `Config.Failed` store, with its connection, queue, full payload, error and time, so you can look at it and retry it later.

The database store needs a `failed_jobs` table:

```
perago make:queue-failed-table --dialect=postgres
```

or `database.Install`, which creates every missing table.

| Store | Constructor |
|---|---|
| Database | `database.NewFailedJobStore(db, "failed_jobs")` |
| DynamoDB | `dynamodb.NewFailedJobStore(client, opts)`, see [below](#storing-failed-jobs-in-dynamodb) |
| Memory | `perago.NewMemoryFailedJobStore()` |
| Disabled | leave `Config.Failed` nil, see [below](#disabling-failed-job-storage) |

Jobs run with `DispatchSync` or the `sync` driver are not stored: their error is returned to you right away.

How many attempts a job gets and how long it waits between them is in [Attempts, timeouts and errors](retries.md):

```
perago queue:work redis --tries=3 --backoff=3
```

## Cleaning up after failed jobs

A `Failed` method runs once, when the job fails for good. Use it to notify someone, or to undo what the job left half done:

```go
func (j *ProcessPodcast) Failed(ctx context.Context, err error) {
	notify.Admins(ctx, "podcast %d failed: %v", j.PodcastID, err)
}
```

{: .warning }
`Failed` receives a **fresh copy** of the job, decoded from the payload, as in Laravel. Changes `Handle` made to the struct are not there.

The error tells you why the job failed. Check it with `perago.IsCode` or `errors.Is`:

```go
func (j *ProcessPodcast) Failed(ctx context.Context, err error) {
	switch {
	case perago.IsCode(err, perago.JobTimeout):          // it timed out
	case perago.IsCode(err, perago.MaxAttemptsExceeded): // it was released until its attempts ran out
	case errors.Is(err, ErrSourceGone):                  // the error Handle returned
	}
}
```

The job's chain `Catch` callbacks, its batch's `Catch`, and `JobFailed` listeners run as well.

## Retrying failed jobs

List the failed jobs, with their ID, connection, queue, name and failure time:

```
perago queue:failed
```

Push one back on its queue, or several, with fresh attempts:

```
perago queue:retry ce7bb17c-cdd8-41f0-a8ec-7b4fef4e5ece
perago queue:retry ce7bb17c-... 91401d2c-...
perago queue:retry --queue=emails     # every failed job of a queue
perago queue:retry all                # every failed job
perago queue:retry-batch <batch-id>   # the failed jobs of a batch
```

Delete one, or all of them:

```
perago queue:forget 91401d2c-0784-4f43-824c-34f94a33c24d
perago queue:flush
perago queue:flush --hours=48         # only those that failed more than 48 hours ago
```

From Go: `m.RetryFailed(ctx, ids...)`, `m.RetryFailedQueue(ctx, queue)`, `m.RetryBatch(ctx, id)`, and `m.FailedJobs()` for `All`, `Find`, `Forget`, `Flush` and `Prune`. Retrying fires `JobRetryRequested`. A retried job gets a fresh retry deadline when it defines `RetryUntil` or `RetryFor`.

Failed jobs are identified by UUID only, so Laravel's `queue:retry --range` has no equivalent.

## Ignoring missing models

A job that holds a migro model through `model.Of[T]` reloads it before `Handle`. When the row was deleted in the meantime, the job fails with `MODEL_NOT_FOUND`. If such a job is simply no longer needed, embed `perago.DeletesMissingModels` (`#[DeleteWhenMissingModels]`) and it is deleted quietly instead:

```go
type SendInvoice struct {
	perago.Queueable
	perago.DeletesMissingModels
	Order model.Of[Order]
}
```

The same works for your own `perago.Restorable` fields: return an error matching `perago.ErrModelNotFound` from `Restore`.

## Pruning failed jobs

```
perago queue:prune-failed              # older than 24 hours
perago queue:prune-failed --hours=48
```

Schedule it daily, with cron, a Kubernetes CronJob or a ticker in your application.

## Storing failed jobs in DynamoDB

```
go get github.com/LouisRiverstone/perago/dynamodb
```

Create the table yourself, usually named `failed_jobs`, with a string partition key `application` (the `Options.Application` value) and a string sort key `uuid`:

```go
client := awsdynamodb.NewFromConfig(cfg) // AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION...

m, err := perago.New(perago.Config{
	// ...
	Failed: dynamodb.NewFailedJobStore(client, dynamodb.Options{
		Table:       "failed_jobs",
		Application: "myapp",
		TTL:         30 * 24 * time.Hour, // optional: expire entries after 30 days
	}),
})
```

With `TTL` set, turn on DynamoDB TTL on the `ttl` attribute and old entries remove themselves.

## Disabling failed job storage

Leave `Config.Failed` nil (Laravel's `null` driver) and failed jobs are discarded after their `Failed` method and events run. `perago.NullFailedJobStore{}` does the same explicitly.

## Failed job events

To run code whenever any job fails, for instance to alert your team, register a `Failing` listener (`Queue::failing`) at startup:

```go
m.Failing(func(ctx context.Context, e perago.JobFailed) {
	slog.Error("job failed",
		"job", e.Job.DisplayName(),
		"uuid", e.Job.UUID(),
		"connection", e.Connection,
		"queue", e.Job.Queue(),
		"err", e.Err)
})
```

`e.Job` is the job's `*perago.Runtime`, `e.Job.Payload()` its decoded payload, and `e.Err` the error.

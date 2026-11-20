---
title: Failed jobs
nav_order: 10
description: "Storing, listing, retrying, forgetting and pruning failed jobs."
---

# Failed jobs

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

A job that fails for good is deleted from its queue and written to the `Config.Failed` store, together with its payload and error. That happens when its attempts run out, its retry deadline passes, it hits `MaxExceptions`, it times out with `FailOnTimeout`, it matches `DontRetry`, or it calls `Fail`.

| Store | Constructor |
|---|---|
| Database | `database.NewFailedJobStore(db, "failed_jobs")`; table from `perago make:queue-failed-table` |
| DynamoDB | `dynamodb.NewFailedJobStore(client, dynamodb.Options{Table: "failed_jobs"})` |
| Memory | `perago.NewMemoryFailedJobStore()` |
| Disabled | leave `Config.Failed` nil (Laravel's `null` driver) |

The DynamoDB table needs a string partition key `application` and a string sort key `uuid`. Set `Options.TTL` and turn on DynamoDB TTL on the `ttl` attribute to expire old entries.

## Cleaning up after failed jobs

```go
func (j *ProcessPodcast) Failed(ctx context.Context, err error) {
	notify.Admins(ctx, "podcast %d failed: %v", j.PodcastID, err)
}
```

`Failed` gets a fresh copy of the job, decoded from the payload. It does not see the changes `Handle` made to the struct.

## Retrying failed jobs

```
perago queue:failed                 # list them
perago queue:retry <uuid> <uuid>    # push them back onto their queue, with fresh attempts
perago queue:retry all
perago queue:retry --queue=emails   # every failed job of a queue
perago queue:retry-batch <batch-id> # the failed jobs of a batch
perago queue:forget <uuid>          # delete one
perago queue:flush                  # delete them all
perago queue:flush --hours=48       # ... those older than 48 hours
```

From Go: `m.RetryFailed(ctx, ids...)`, `m.RetryFailedQueue(ctx, queue)`, `m.RetryBatch(ctx, id)` and `m.FailedJobs()`. A retried job gets a fresh retry deadline when it defines `RetryUntil` or `RetryFor`.

## Ignoring missing models

See [models](jobs.md#models): with `perago.DeletesMissingModels`, a job whose model was deleted is discarded instead of failing.

## Pruning failed jobs

```
perago queue:prune-failed              # older than 24 hours
perago queue:prune-failed --hours=48
```

## Failed job events

```go
m.Failing(func(ctx context.Context, e perago.JobFailed) {
	log.Printf("%s on %s failed: %v", e.Job.DisplayName(), e.Connection, e.Err)
})
```

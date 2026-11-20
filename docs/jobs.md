---
title: Jobs
nav_order: 4
description: "Job structs, registration, options, unique, debounced and encrypted jobs, and models."
---

# Jobs

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Generating jobs

```
perago make:job ProcessPodcast            # app/jobs/process_podcast.go
perago make:job ProcessPodcast --batched  # checks for a cancelled batch
perago make:job SendSms --sync            # runs in the dispatching process
```

## Class structure

A job is any type with `Handle(ctx context.Context) error`. Its **exported fields are its payload**: they are JSON-encoded when the job is dispatched and decoded into a fresh value before `Handle` runs. Embed `perago.Queueable` to get the queue methods (`Release`, `Delete`, `Fail`, `Attempts`, `Batch`, `OnQueue`, and the rest).

```go
type ProcessPodcast struct {
	perago.Queueable
	PodcastID int
	Mailer    *mail.Client `json:"-"` // not part of the payload
}

func init() { perago.Register(&ProcessPodcast{}) }

func (j *ProcessPodcast) Handle(ctx context.Context) error { ... }
```

Workers rebuild jobs by name, so **register every job in `init()`** of a package your worker imports. The name defaults to `package.Type`. Declare `JobName() string`, or use `perago.RegisterAs("podcasts.Process", &ProcessPodcast{})`, to keep old payloads working after a rename.

To inject dependencies, as Laravel does through `handle`'s parameters, register a factory:

```go
perago.RegisterFactory("jobs.ProcessPodcast", func() perago.Job {
	return &ProcessPodcast{Mailer: mailer}
})
```

Binary data goes into a `[]byte` field, which JSON stores as base64.

### Models

`model.Of[T]` is Laravel's `SerializesModels` for migro models. Only the primary key travels, and the worker loads the row again, fresh, before `Handle`. Relations are never serialized, which matches Laravel's `withoutRelations`. List the ones to load again with `With`:

```go
type SendInvoice struct {
	perago.DeletesMissingModels          // #[DeleteWhenMissingModels]
	Order model.Of[Order]
	Lines model.Many[Line]
}

model.Use(db) // once, at startup
m.Dispatch(&SendInvoice{Order: model.New(order).With("Customer")}).Send(ctx)

func (j *SendInvoice) Handle(ctx context.Context) error {
	order := j.Order.Get()
	...
}
```

When the row is gone, the job fails with `MODEL_NOT_FOUND`. If the job embeds `perago.DeletesMissingModels` (or returns true from `DeleteWhenMissingModels()`), it is deleted quietly instead. Any field type can join in by implementing `perago.Restorable`.

## Job options

Laravel's attributes and properties are optional methods on the job:

| Laravel | perago |
|---|---|
| `#[Tries(5)]`, `$tries` | `Tries() int` (0 retries forever) |
| `#[Backoff([10, 30])]`, `backoff()` | `Backoff() []time.Duration` (the last value repeats) |
| `#[Timeout(120)]` | `Timeout() time.Duration` |
| `retryUntil()` | `RetryUntil() time.Time` (wins over Tries) |
| `#[RetryFor(minutes: 10)]` | `RetryFor() time.Duration` |
| `#[MaxExceptions(3)]` | `MaxExceptions() int` |
| `#[FailOnTimeout]` | `FailOnTimeout() bool` or embed `perago.FailsOnTimeout` |
| `#[CountCrashesAsExceptions]` | `CountCrashesAsExceptions() bool` or embed `perago.CountsCrashes` |
| `#[DeleteWhenMissingModels]` | `DeleteWhenMissingModels() bool` or embed `perago.DeletesMissingModels` |
| `failed(Throwable $e)` | `Failed(ctx, err error)` |
| `middleware()` | `Middleware() []perago.Middleware` |
| `displayName()` | `DisplayName() string` |
| `messageGroup()`, `deduplicationId()` | `MessageGroup() string`, `DeduplicationID() string` |

A job without `Tries` uses the worker's `--tries` (1 by default). Attempts are counted the way Laravel counts them: each reservation is an attempt, whether it ends in an error, a release, a timeout or success.

## Unique jobs

```go
type UpdateSearchIndex struct {
	perago.Unique       // ShouldBeUnique
	ProductID int
}

func (j *UpdateSearchIndex) UniqueID() string         { return strconv.Itoa(j.ProductID) }
func (j *UpdateSearchIndex) UniqueFor() time.Duration { return time.Hour } // #[UniqueFor(3600)]
func (j *UpdateSearchIndex) UniqueVia() perago.Cache  { return redisCache } // optional
```

While a unique job is queued or running, dispatching another one with the same `UniqueID` does nothing: `Send` returns an empty id and a nil error. The lock is released when the job finishes or fails. With `perago.UniqueUntilProcessing`, it is released right before the job starts. Unique locks need an atomic `Cache`: memory (one process), database or Redis. They do not apply to jobs inside a batch.

## Debounced jobs

```go
func (j *ReindexDocument) DebounceFor() time.Duration     { return 30 * time.Second } // #[DebounceFor(30)]
func (j *ReindexDocument) DebounceMaxWait() time.Duration { return 2 * time.Minute }  // maxWait: 120
func (j *ReindexDocument) DebounceID() string             { return j.DocumentID }
func (j *ReindexDocument) DebounceVia() perago.Cache       { return redisCache }       // optional
```

Within the window, only the latest dispatch runs. The superseded jobs are deleted when they come up, and each fires `JobDebounced`. `DebounceMaxWait` caps how long a job can keep being pushed back. Debounce and unique are mutually exclusive.

## Encrypted jobs

```go
type ChargeCard struct {
	perago.Encrypted   // ShouldBeEncrypted
	Card string
}
```

The payload is sealed with AES-256-GCM using `Config.Key` (32 bytes) before it reaches the backend. To rotate the key, move the old one to `Config.PreviousKeys`.

## Interacting with the queue

Inside `Handle`, through the embedded `Queueable`:

```go
j.Attempts()                  // this attempt included
j.Release()                   // back on the queue now
j.Release(10 * time.Second)   // ... after a delay
j.ReleaseUsingBackoff()       // ... after the backoff for this attempt
j.Delete()                    // remove it; a chain still continues
j.Fail()                      // fail now, without retrying
j.Fail(err)                   // ... with a cause
j.Fail("bad input")           // ... with a message
j.JobID(); j.BatchID(); j.Batch(ctx)
```

Middleware and code that only has the context can use `rt, _ := perago.JobFrom(ctx)` to reach the same runtime.

To stop retrying on specific errors everywhere, use `m.DontRetry(ErrInvalid)` or `m.DontRetryWhen(func(err error) bool {...})`. For a single job, use the [`FailOnException`](middleware.md#failing-on-specific-errors) middleware.

## Interruptible jobs

A worker that receives SIGTERM, SIGINT or SIGQUIT finishes its current job before it exits. To stop early, implement `Interrupted(sig os.Signal)`:

```go
func (j *ImportProducts) Interrupted(os.Signal) { j.stop.Store(true) }
```

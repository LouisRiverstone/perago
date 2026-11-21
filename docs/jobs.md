---
title: Creating jobs
nav_order: 5
description: "Generating jobs, their structure, how they are serialized, dependency injection, models, unique, debounced and encrypted jobs."
---

# Creating jobs

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Generating job classes

Jobs live in `app/jobs` by default. The directory is created the first time you run `make:job`:

```
perago make:job ProcessPodcast            # app/jobs/process_podcast.go
perago make:job ImportCsv --batched       # starts with the "batch cancelled?" check
perago make:job SendSms --sync            # runs in the dispatching process (ShouldNotQueue)
```

`App.JobsDir` changes the directory. The generated file is ordinary Go code, so edit it freely; there are no stubs to publish.

## Class structure

A job is any type with a `Handle(ctx context.Context) error` method. A generated job looks like this:

```go
package jobs

import (
	"context"

	"github.com/LouisRiverstone/perago"

	"example.com/app/internal/podcasts"
)

type ProcessPodcast struct {
	perago.Queueable
	PodcastID int64
}

func init() { perago.Register(&ProcessPodcast{}) }

func (j *ProcessPodcast) Handle(ctx context.Context) error {
	p, err := podcasts.Find(ctx, j.PodcastID)
	if err != nil {
		return err
	}
	return p.Process(ctx)
}
```

- **`perago.Queueable`** is Laravel's `Queueable`, `InteractsWithQueue` and `Batchable` traits in one embedded struct: `Release`, `Delete`, `Fail`, `Attempts`, `Batch`, `PrependToChain`, the `OnQueue`/`OnConnection` defaults and the rest. It is never serialized.
- **The exported fields are the payload.** Here only `PodcastID` travels through the queue.
- **`Handle`** runs on the worker. The `ctx` it receives is cancelled when the job times out or the worker shuts down; pass it to every call that blocks.
- **The error `Handle` returns** is Laravel's thrown exception: the job is released and retried with its backoff until its attempts run out, then it fails. Returning `nil` means success. A panic is recovered and treated as an error.

### How jobs are serialized

Like Laravel, perago does not run your job where you dispatch it. It turns the job into a string, stores the string in the queue, and a worker, possibly on another machine, turns it back into a job and calls `Handle`. Laravel uses PHP's `serialize()`; perago uses JSON.

When you call `m.Dispatch(&ProcessPodcast{PodcastID: 42}).Send(ctx)`, perago:

1. runs `PrepareForDispatch`, if the job has one, and checks unique and debounce locks;
2. encodes the job struct with `json.Marshal`. That is the `data` field below;
3. reads the job's options (`Tries`, `Backoff`, `Timeout`, `RetryUntil`, ...) and stores them next to the data, resolved at dispatch time, as Laravel does;
4. seals `data` with AES-256-GCM when the job is [encrypted](#encrypted-jobs);
5. hands the envelope to the connection's driver: a row in `jobs`, a Redis list entry, an SQS message, a Beanstalkd job.

The envelope stored in the queue looks like this:

```json
{
  "uuid": "8f6a4c1e-...",
  "displayName": "jobs.ProcessPodcast",
  "job": "jobs.ProcessPodcast",
  "data": {"PodcastID": 42},
  "maxTries": 5,
  "backoff": [10000, 60000],
  "timeout": 120000,
  "connection": "database",
  "queue": "podcasts",
  "createdAt": 1800000000
}
```

Chained jobs travel inside `chain`, and batched jobs carry their `batchId`. On the worker, perago:

1. reads `job` and looks the name up in the registry;
2. builds an empty job with the registered factory and runs `json.Unmarshal` on `data`;
3. calls `Restore` on every field that implements `perago.Restorable` (the way `SerializesModels` reloads models);
4. runs the job's middleware, then `Handle`.

Because the worker rebuilds jobs by name, **every job type must be registered**, in an `init()` of a package your worker binary imports:

```go
func init() { perago.Register(&ProcessPodcast{}) }
```

The name defaults to `package.Type` (`jobs.ProcessPodcast`). A worker that pops a name it does not know fails the job with `JOB_NOT_REGISTERED` rather than guessing. To rename a type without breaking jobs already queued, keep the old name with a `JobName() string` method or `perago.RegisterAs("jobs.ProcessPodcast", &RenamedJob{})`.

What survives the trip follows `encoding/json`:

| Field | In the payload? |
|---|---|
| Exported fields (`PodcastID`) | Yes |
| Unexported fields (`podcastID`) | No: they are zero on the worker |
| Fields tagged `json:"-"` | No: use them for dependencies (see below) |
| `time.Time`, `[]byte`, maps, slices, nested structs, pointers | Yes (`[]byte` as base64) |
| Interface-typed fields | Encoded, but cannot be decoded into an interface; use a concrete type |
| Functions and channels | No: the dispatch fails with `PAYLOAD_INVALID` |
| Types with `MarshalJSON`/`UnmarshalJSON` | Yes, in their own format |

Keep payloads small: IDs, strings and numbers. Load records in `Handle`, so the job sees the current data and not a copy taken at dispatch time. That is also Laravel's advice, and why `SerializesModels` stores only the key.

### `Handle` method dependency injection

Laravel injects `handle`'s parameters from the service container, and `bindMethod` lets you build them yourself. In Go, a job's dependencies are fields that are not part of the payload, filled by a factory you register instead of `perago.Register`:

```go
type ProcessPodcast struct {
	perago.Queueable
	PodcastID int64

	Processor *audio.Processor `json:"-"` // never serialized
}

// at startup, once the dependencies exist
perago.RegisterFactory("jobs.ProcessPodcast", func() perago.Job {
	return &ProcessPodcast{Processor: processor}
})
```

The worker calls the factory, then decodes the payload into the value it returns, so the job arrives in `Handle` with both its data and its dependencies. The dispatching side does not need the factory: it only encodes the exported fields.

### Binary data

Laravel asks you to `base64_encode` binary data before passing it to a job. A `[]byte` field is base64-encoded by `encoding/json` for you:

```go
type StoreThumbnail struct {
	perago.Queueable
	Image []byte
}
```

Large blobs still make large payloads: SQS caps messages at 1 MiB (see [overflow storage](drivers.md#sqs-overflow-storage)) and every driver has to move the bytes around. Prefer storing the file and passing its path.

### Models

Laravel serializes an Eloquent model as its class and key, and loads it again, with its loaded relations, before `handle`. On a migro project, `model.Of[T]` and `model.Many[T]` from the [`perago/migro`](migro.md#models-in-jobs) module do the same:

```go
type SendInvoice struct {
	perago.Queueable
	Order model.Of[Order]
}

m.Dispatch(&SendInvoice{Order: model.New(order)}).Send(ctx)
```

Only the primary key goes into the payload. The worker loads the row right before `Handle`, so the job sees the current record. When the row is gone, the job fails with `MODEL_NOT_FOUND`, unless it [ignores missing models](failed-jobs.md#ignoring-missing-models).

Without migro, store the key and load the record in `Handle`, or give any field type a `Restore(ctx) error` method (`perago.Restorable`). The worker calls it after decoding, before the middleware run.

### Queued relationships

Laravel serializes the relations loaded on a model and reloads them without their constraints, which is why it offers `withoutRelations` and `#[WithoutRelations]`. perago never serializes relations: `model.Of` keeps only the key, which matches Laravel with `#[WithoutRelations]` on every model. To load relations again on the worker, name them:

```go
m.Dispatch(&SendInvoice{Order: model.New(order).With("Customer", "Lines")}).Send(ctx)
```

They are loaded in full, as Laravel does. If you need a subset (only the unpaid lines, say), load it in `Handle` with your own query. `model.Many[T]` accepts `With` too, unlike Laravel's collections, whose relations are not restored.

## Unique jobs

{: .note }
Unique jobs need an atomic cache shared by every process that dispatches: `database.NewCache` or `redis.NewCache`. The default in-process `MemoryCache` works only when one process dispatches. Unique locks do not apply to jobs inside a batch.

Sometimes only one copy of a job should be on the queue at a time. Embed `perago.Unique` (`ShouldBeUnique`) and say what makes two jobs the same:

```go
type UpdateSearchIndex struct {
	perago.Queueable
	perago.Unique
	ProductID int64
}

func (j *UpdateSearchIndex) UniqueID() string         { return strconv.FormatInt(j.ProductID, 10) }
func (j *UpdateSearchIndex) UniqueFor() time.Duration { return time.Hour } // #[UniqueFor(3600)]
```

While an `UpdateSearchIndex` for product 1 is queued or running, dispatching another one for product 1 does nothing: `Send` returns an empty id and a nil error. Without `UniqueID`, the job type alone is the key, so only one job of that type at a time. `UniqueFor` releases the lock after that long even if the job is still around, so a lost job cannot block new ones forever; without it, the lock lasts until the job is done.

### Keeping jobs unique until processing begins

By default, the lock is released when the job finishes, or when it fails after its last attempt. Embed `perago.UniqueUntilProcessing` (`ShouldBeUniqueUntilProcessing`) instead to release it right before the job starts, so a new copy can be queued while the current one runs:

```go
type UpdateSearchIndex struct {
	perago.Queueable
	perago.UniqueUntilProcessing
	ProductID int64
}
```

### Unique job locks

On dispatch, perago takes a lock named after the job type and `UniqueID` in the Manager's cache, and releases it as described above. `UniqueVia` picks another cache for one job type:

```go
func (j *UpdateSearchIndex) UniqueVia() perago.Cache { return redisCache }
```

{: .note }
To limit how many copies run at the same time, rather than how many are queued, use the [`WithoutOverlapping`](middleware.md#preventing-job-overlaps) middleware.

## Debounced jobs

Debouncing runs only the **latest** dispatch of a job within a window. Saving a document ten times in five seconds then reindexes it once, with its final content:

```go
type ReindexDocument struct {
	perago.Queueable
	DocumentID string
}

func (j *ReindexDocument) DebounceFor() time.Duration { return 30 * time.Second } // #[DebounceFor(30)]
func (j *ReindexDocument) DebounceID() string         { return j.DocumentID }    // debounceId()
```

Each dispatch is queued with a 30 second delay and becomes the current one. When an older dispatch comes up, it sees it was superseded, is deleted without running, and fires `JobDebounced`.

A document that is saved every few seconds could be pushed back forever. `DebounceMaxWait` caps the wait (`#[DebounceFor(30, maxWait: 120)]`): after two minutes from the first dispatch, the current job runs even if new dispatches keep coming.

```go
func (j *ReindexDocument) DebounceMaxWait() time.Duration { return 2 * time.Minute }
func (j *ReindexDocument) DebounceVia() perago.Cache       { return redisCache } // debounceVia(), optional
```

Debouncing uses the cache like unique jobs, with the same need for a shared one. A job cannot be both debounced and unique.

## Encrypted jobs

When a job carries something sensitive, embed `perago.Encrypted` (`ShouldBeEncrypted`):

```go
type ChargeCard struct {
	perago.Queueable
	perago.Encrypted
	CardToken string
}
```

The job's data is sealed with AES-256-GCM using `Config.Key` (32 bytes, like `APP_KEY`) before it reaches the driver: the queue stores a `secret` field instead of `data`. The worker opens it with the same key. A missing key makes the dispatch fail with `INVALID_CONFIG`.

To rotate keys, set the new one in `Config.Key` and move the old ones to `Config.PreviousKeys` (Laravel's `APP_PREVIOUS_KEYS`). New jobs use the new key, and jobs queued before the rotation still open. A payload that no key opens fails with `PAYLOAD_DECRYPT_FAILED`.

## Job options

Laravel's attributes and properties are optional methods on the job. They are read at dispatch and stored in the payload. [Attempts, timeouts and errors](retries.md) explains each one:

| Laravel | perago |
|---|---|
| `#[Tries(5)]`, `$tries`, `tries()` | `Tries() int` (0 retries forever) |
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
| `ShouldQueueAfterCommit` | embed `perago.QueueAfterCommit` |
| a job without `ShouldQueue` | embed `perago.Sync` (`make:job --sync`) |

## Interacting with the queue

Inside `Handle`, the embedded `Queueable` reaches the job being processed:

```go
j.Attempts()                  // this attempt included, starting at 1
j.Release()                   // back on the queue now
j.Release(10 * time.Second)   // ... after a delay
j.ReleaseUsingBackoff()       // ... after the backoff for this attempt
j.Delete()                    // remove it; a chain still continues
j.Fail()                      // fail now, without retrying
j.Fail(err)                   // ... with a cause
j.Fail("bad input")           // ... with a message
j.JobID(); j.BatchID(); j.Batch(ctx)
```

Middleware and code that only has the context use `rt, ok := perago.JobFrom(ctx)` to reach the same `*perago.Runtime`. See [manually releasing](retries.md#manually-releasing-a-job) and [manually failing](retries.md#manually-failing-a-job) a job.

## Interruptible jobs

A worker that receives SIGTERM, SIGINT or SIGQUIT finishes its current job before it exits. A long job can stop early by implementing `Interrupted(sig os.Signal)` (`Interruptible`):

```go
func (j *ImportProducts) Interrupted(os.Signal) { j.stop.Store(true) }
```

See [reacting to worker signals](workers.md#reacting-to-worker-signals).

---
title: Coming from Laravel
nav_order: 16
description: "Laravel queue code and its perago equivalent."
---

# Coming from Laravel

perago follows [Laravel's queue documentation](https://laravel.com/docs/queues) (13.x) section by section. The first table tells you where each section lives here; the second translates the code.

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Laravel's queue docs, section by section

| Laravel section | perago |
|---|---|
| Introduction: connections vs. queues | [Getting started](getting-started.md#connections-and-queues) |
| Driver notes and prerequisites: database, Redis, Redis Cluster, blocking, SQS overflow storage | [Connections and drivers](drivers.md) |
| Creating jobs: generating, class structure, `handle` injection, queued relationships | [Creating jobs](jobs.md#class-structure) |
| Unique jobs, debounced jobs, encrypted jobs | [Creating jobs](jobs.md#unique-jobs) |
| Job middleware: rate limiting, preventing overlaps, throttling exceptions, releasing, skipping | [Job middleware](middleware.md) |
| Dispatching jobs: delayed, synchronous, deferred, bulk, preparing, transactions | [Dispatching jobs](dispatching.md) |
| Job chaining | [Chains](chains.md) |
| Customizing the queue and connection, routing, forwarding | [Dispatching jobs](dispatching.md#customizing-the-queue-and-connection) |
| Max job attempts and timeout values | [Attempts, timeouts and errors](retries.md) |
| SQS FIFO and fair queues | [Dispatching jobs](dispatching.md#sqs-fifo-and-fair-queues) |
| Queue failover | [Dispatching jobs](dispatching.md#queue-failover) |
| Error handling: releasing and failing manually | [Attempts, timeouts and errors](retries.md#error-handling) |
| Job batching | [Batches](batches.md) |
| Queueing closures | [Dispatching jobs](dispatching.md#queueing-closures) |
| Running the queue worker, priorities, deployment, signals, expiration, pausing | [Running workers](workers.md) |
| Supervisor configuration | [Running workers](workers.md#supervisor-configuration) |
| Dealing with failed jobs | [Failed jobs](failed-jobs.md) |
| Clearing jobs from queues, monitoring your queues | [Events and monitoring](events.md#monitoring-your-queues) |
| Testing | [Testing](testing.md) |
| Job events | [Events and monitoring](events.md#job-events) |

## Code, side by side

| Laravel | perago |
|---|---|
| `class X implements ShouldQueue { use Queueable; }` | `type X struct { perago.Queueable }` + `perago.Register(&X{})` |
| `handle()` | `Handle(ctx) error` |
| constructor properties | exported struct fields (JSON) |
| `SerializesModels`, Eloquent model property | `model.Of[T]`, `model.Many[T]` |
| `X::dispatch($a)` | `m.Dispatch(&X{A: a}).Send(ctx)` |
| `dispatchIf`, `dispatchUnless`, `dispatchSync` | `m.DispatchIf`, `m.DispatchUnless`, `m.DispatchSync` |
| `dispatchAfterResponse`, `deferred` / `background` connections | `m.DispatchAfterResponse`, `perago.DeferredDriver()`, `perago.BackgroundDriver()` |
| `->delay(now()->plus(minutes: 10))` | `.Delay(10 * time.Minute)` |
| `->onQueue()`, `->onConnection()`, `->onGroup()` | `.OnQueue()`, `.OnConnection()`, `.OnGroup()` |
| `->afterCommit()`, `->beforeCommit()`, `after_commit` | `.AfterCommit()`, `.BeforeCommit()`, `Connection.AfterCommit` |
| `Queue::route(...)`, `Queue::forward(...)` | `m.Route`, `perago.RouteInterface`, `m.Forward` |
| `Bus::chain([...])->catch()->dispatch()` | `m.Chain(...).Catch(fn).Dispatch(ctx)` |
| `Bus::batch([...])->then()->catch()->finally()->dispatch()` | `m.Batch(...).Then().Catch().Finally().Dispatch(ctx)` |
| `Bus::bulk($jobs)` | `m.Bulk(ctx, jobs...)` |
| `Bus::findBatch($id)` | `m.FindBatch(ctx, id)` |
| `$this->release(10)`, `releaseUsingBackoff()` | `j.Release(10 * time.Second)`, `j.ReleaseUsingBackoff()` |
| `$this->fail($e)`, `$this->delete()`, `$this->attempts()` | `j.Fail(err)`, `j.Delete()`, `j.Attempts()` |
| `$this->batch()` | `j.Batch(ctx)` |
| `$this->prependToChain()`, `appendToChain()` | `j.PrependToChain()`, `j.AppendToChain()` |
| `#[Tries]`, `#[Backoff]`, `#[Timeout]`, `#[MaxExceptions]` | `Tries()`, `Backoff()`, `Timeout()`, `MaxExceptions()` |
| `retryUntil()`, `#[RetryFor]` | `RetryUntil()`, `RetryFor()` |
| `#[FailOnTimeout]`, `#[CountCrashesAsExceptions]` | embed `perago.FailsOnTimeout`, `perago.CountsCrashes` |
| `#[DeleteWhenMissingModels]` | embed `perago.DeletesMissingModels` |
| `ShouldBeUnique`, `ShouldBeUniqueUntilProcessing`, `uniqueId()`, `#[UniqueFor]` | embed `perago.Unique` / `perago.UniqueUntilProcessing`, `UniqueID()`, `UniqueFor()` |
| `#[DebounceFor]`, `debounceId()` | `DebounceFor()`, `DebounceMaxWait()`, `DebounceID()` |
| `ShouldBeEncrypted` | embed `perago.Encrypted` |
| `PreparesForDispatch` | `PrepareForDispatch(ctx) bool` |
| `Interruptible::interrupted($signal)` | `Interrupted(os.Signal)` |
| `RateLimiter::for()` + `RateLimited` | `m.RateLimiter()` + `middleware.RateLimited` |
| `WithoutOverlapping`, `ThrottlesExceptions`, `FailOnException`, `Skip`, `Release` | same names in `middleware` |
| `$exceptions->dontRetry()` | `m.DontRetry()`, `m.DontRetryWhen()` |
| `Queue::before/after/failing/looping` | `m.Before/After/Failing/Looping` |
| `Event::listen(QueueBusy::class)` | `perago.Listen(m, func(ctx, perago.QueueBusy))` |
| `Queue::withoutInterruptionPolling()` | `m.WithoutInterruptionPolling()` |
| `Queue::fake()`, `Bus::fake()` | `queuetest.Fake(m)` |
| `Queue::assertPushed(X::class, fn)` | `queuetest.AssertPushed(t, fake, func(j *X) bool {...})` |
| `$job->withFakeQueueInteractions()` | `queuetest.WithFakeQueueInteractions(job)` |
| `$job->withFakeBatch()` | `queuetest.WithFakeBatch(job)` |
| `php artisan queue:work` | `perago queue:work` |

## What differs

- **Closures** cannot be serialized in Go. `perago.Closure` runs on in-process connections only. Use `RegisterClosure` and `Call` for closures that workers in other processes run. Chain and batch callbacks given as closures run when the worker that finishes them is the dispatching process; use the `*Job` variants otherwise.
- **Timeouts** cancel the job's context. A job that ignores it makes the worker exit after `Grace`, as Laravel's worker kills itself.
- **Failed jobs** are identified by UUID only, so `queue:retry --range` has no equivalent.
- **Horizon** and the MongoDB driver are not part of Laravel's core and are not included.

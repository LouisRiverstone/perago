---
title: Coming from Laravel
nav_order: 15
description: "Laravel queue code and its perago equivalent."
---

# Coming from Laravel

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

---
title: Attempts, timeouts and errors
nav_order: 9
description: "Max attempts, time-based attempts, max exceptions, stopping retries, timeouts, backoff, and releasing or failing a job by hand."
---

# Attempts, timeouts and errors

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

A job that returns an error is retried. How many times, how far apart, and for how long it may run are set per worker or per job; the job's setting wins.

| Setting | Worker flag | Job method |
|---|---|---|
| Attempts | `--tries=3` | `Tries() int` |
| Retry deadline | | `RetryUntil() time.Time`, `RetryFor() time.Duration` |
| Unhandled errors allowed | | `MaxExceptions() int` |
| Delay between attempts | `--backoff=10,30` | `Backoff() []time.Duration` |
| Time a job may run | `--timeout=60` | `Timeout() time.Duration` |
| Fail on the first timeout | | `FailOnTimeout() bool` |

The job's values are read when it is dispatched and stored in its payload, so a worker with other flags still honours them.

## Max attempts

A job is attempted **once** by default. An attempt is used every time a worker reserves the job, whatever the outcome:

- `Handle` returns an error or panics;
- the job releases itself with `j.Release`, or a middleware releases it (`WithoutOverlapping`, `RateLimited`, `ThrottlesExceptions`, `ReleaseWhen`);
- the job times out;
- `Handle` returns nil.

When a job comes back after its last attempt, it fails with `MAX_ATTEMPTS_EXCEEDED` and goes to [failed jobs](failed-jobs.md).

For every job a worker runs:

```
perago queue:work --tries=3
```

For one job type, which wins over the flag:

```go
func (j *ProcessPodcast) Tries() int { return 5 } // #[Tries(5)]
```

`Tries() int { return 0 }` and `--tries=0` retry forever. Use that together with `RetryUntil` or `MaxExceptions`, so a broken job still stops at some point.

## Time based attempts

Instead of a number of attempts, a job can say until when it may be retried. `RetryUntil` wins over `Tries` when both are defined:

```go
func (j *ProcessPodcast) RetryUntil() time.Time { return time.Now().Add(10 * time.Minute) }
```

`RetryUntil` is called at dispatch time, so the deadline is ten minutes after the dispatch. `RetryFor` (`#[RetryFor]`) says the same thing as a duration:

```go
func (j *ProcessPodcast) RetryFor() time.Duration { return 90 * time.Minute } // #[RetryFor(hours: 1, minutes: 30)]
```

When both exist, `RetryUntil` is used. A job retried from `failed_jobs` gets a fresh deadline.

## Max exceptions

A job may be released many times by design (rate limits, overlap locks) but should still give up when it keeps crashing. `MaxExceptions` counts only the errors `Handle` returns, not releases:

```go
func (j *ProcessPodcast) Tries() int         { return 25 } // released up to 25 times
func (j *ProcessPodcast) MaxExceptions() int { return 3 }  // but fails at the third error

func (j *ProcessPodcast) Handle(ctx context.Context) error {
	ok, err := lock.TryAcquire(ctx, "podcast")
	if err != nil {
		return err // counts towards MaxExceptions
	}
	if !ok {
		j.Release(10 * time.Second) // does not count
		return nil
	}
	defer lock.Release(ctx)
	return process(ctx, j.PodcastID)
}
```

When the error count reaches `MaxExceptions`, the job fails with `MAX_EXCEPTIONS_EXCEEDED`.

A worker that dies in the middle of a job (out of memory, `kill -9`) returns no error, so by default that attempt does not count as an exception. Embed `perago.CountsCrashes` (`#[CountCrashesAsExceptions]`) to count it: the worker writes a marker in the cache while the job runs, and finds it the next time the job comes up.

```go
type ProcessPodcast struct {
	perago.Queueable
	perago.CountsCrashes
	PodcastID int64
}
```

## Stopping retries by error

Some errors will never go away on a retry: a validation error, a missing record. Tell the Manager to fail them at once, for every job:

```go
var ErrInvalidSource = errors.New("invalid podcast source")

m.DontRetry(ErrInvalidSource)          // $exceptions->dontRetry([...]), matched with errors.Is
m.DontRetryWhen(func(err error) bool { // $exceptions->dontRetryWhen(...)
	var apiErr *api.Error
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusForbidden
})
```

For one job type, use the [`FailOnException`](middleware.md#failing-on-specific-errors) middleware.

## Backoff

Without a backoff, a failed job is retried as soon as a worker is free. Set a delay per worker:

```
perago queue:work --backoff=3          # 3 seconds before each retry
perago queue:work --backoff=10,30,60   # 10s, 30s, then 60s for every later retry
```

or per job:

```go
func (j *ProcessPodcast) Backoff() []time.Duration { // #[Backoff([1, 5, 10])]
	return []time.Duration{time.Second, 5 * time.Second, 10 * time.Second}
}
```

The first retry waits the first value, the second retry the second one, and the last value repeats for every retry after that. A single value is a fixed delay.

## Timeout

A job that hangs would hold its worker forever. The worker gives each job **60 seconds** by default:

```
perago queue:work --timeout=30
perago queue:work --timeout=0     # no limit
```

A job can choose its own, which wins over the flag:

```go
func (j *ProcessPodcast) Timeout() time.Duration { return 2 * time.Minute } // #[Timeout(120)]
```

When the time is up, the job's context is cancelled and the attempt counts as failed with `JOB_TIMEOUT` and fires `JobTimedOut`. It is then retried with its backoff like any error, or failed for good once its attempts are used up.

Laravel kills the worker process, because PHP cannot interrupt a running job. Go cannot kill a goroutine either, so perago relies on the context: **pass `ctx` to everything that blocks** (database calls, HTTP requests, `select` loops). A job that ignores its context and has not returned `WorkerOptions.Grace` (5 seconds) after the timeout makes the worker exit with status 1, so that your process manager starts a fresh one. That mirrors Laravel's behavior.

Also give your I/O its own timeouts: an `http.Client{Timeout: ...}`, connection timeouts in your DSNs. A socket that never answers is the usual reason a job hangs.

{: .warning }
Keep every timeout **shorter than the connection's `retry_after`** (`RetryAfter`, visibility timeout, TTR). After `retry_after`, the job is handed to another worker while the first one is still running it. See [job expiration](workers.md#job-expiration-and-timeouts).

### Failing on timeout

By default a timed-out job uses an attempt and is released. Embed `perago.FailsOnTimeout` (`#[FailOnTimeout]`) to fail it on the first timeout, whatever its `Tries`:

```go
type ProcessPodcast struct {
	perago.Queueable
	perago.FailsOnTimeout
	PodcastID int64
}
```

## Error handling

When `Handle` returns an error, perago:

1. fires `JobExceptionOccurred`;
2. checks `DontRetry`, `MaxExceptions`, the attempts and the retry deadline;
3. releases the job with its backoff (`JobReleasedAfterException`), or fails it.

A panic is recovered and handled like an error. The error carries the stack trace, so it ends up in `failed_jobs` with the rest.

### Manually releasing a job

A job can put itself back on the queue without returning an error:

```go
func (j *ProcessPodcast) Handle(ctx context.Context) error {
	if !encoderReady() {
		j.Release()                    // available again right away
		// j.Release(10 * time.Second) // after a delay
		return nil
	}
	...
}
```

A release uses an attempt. `ReleaseUsingBackoff` releases with the job's backoff for the current attempt: with `Backoff()` returning `10s, 30s, 60s`, the first attempt waits 10 seconds, the second 30, and every attempt after that 60. A job without a backoff is released right away.

```go
j.ReleaseUsingBackoff()
```

### Manually failing a job

`Fail` marks the job as failed for good, with no more attempts:

```go
j.Fail()                                     // JOB_FAILED_MANUALLY
j.Fail(err)                                  // with the cause, passed to Failed and stored
j.Fail("the podcast's source is gone")       // a message becomes an error
```

Return right after calling it: `Fail` takes effect when `Handle` returns.

### Failing jobs on specific errors

To retry transient errors but fail at once on permanent ones, for a single job type, use the [`FailOnException`](middleware.md#failing-on-specific-errors) middleware:

```go
func (j *SyncChatHistory) Tries() int { return 3 }

func (j *SyncChatHistory) Middleware() []perago.Middleware {
	return []perago.Middleware{middleware.FailOnException(ErrUnauthorized)}
}
```

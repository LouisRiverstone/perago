---
title: Job middleware
nav_order: 6
description: "Wrap job execution: rate limiting, overlap locks, exception throttling, releasing, skipping and your own middleware."
---

# Job middleware

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

Middleware wrap the execution of a job, the way HTTP middleware wrap a handler, so rules like rate limits stay out of `Handle`. Without middleware, a job that may run once every five seconds has to do it itself:

```go
func (j *ProcessPodcast) Handle(ctx context.Context) error {
	ok, err := limiter.Allow(ctx, "podcasts", 1, 5*time.Second)
	if err != nil {
		return err
	}
	if !ok {
		j.Release(5 * time.Second) // try again later
		return nil
	}
	// ... the actual work
	return nil
}
```

The same rule as a middleware:

```go
package middleware

type RateLimit struct{ Limiter *Limiter }

func (m RateLimit) Handle(ctx context.Context, job perago.Job, next perago.Next) error {
	ok, err := m.Limiter.Allow(ctx, "podcasts", 1, 5*time.Second)
	if err != nil {
		return err
	}
	if !ok {
		rt, _ := perago.JobFrom(ctx)
		rt.Release(5 * time.Second)
		return nil
	}
	return next(ctx) // run the job, or the next middleware
}
```

`perago make:job-middleware RateLimit` writes a skeleton like this into `app/jobs/middleware`. A one-off middleware can also be a `perago.MiddlewareFunc`.

Attach middleware by returning them from the job's `Middleware` method. `make:job` does not generate it; add it yourself. They run in order, outermost first:

```go
func (j *ProcessPodcast) Middleware() []perago.Middleware {
	return []perago.Middleware{middleware.RateLimit{Limiter: limiter}}
}
```

A middleware can run the job (`next(ctx)`), skip it (`return nil` after `rt.Delete()`), release it (`rt.Release(d)`), fail it (`rt.Fail(err)`), or return an error, which counts like an error from `Handle`.

The built-in middleware are in `github.com/LouisRiverstone/perago/middleware`. Their state (counters, locks) lives in the Manager's `Cache` unless `Via(cache)` points them elsewhere, so with several workers the cache must be shared: database or Redis.

## Rate limiting

Define named rate limiters once, at startup. A limiter receives the job and returns its limits:

```go
m.RateLimiter("backups", func(job perago.Job) []perago.Limit {
	b := job.(*Backup)
	if b.User.Premium {
		return []perago.Limit{perago.NoLimit()}
	}
	return []perago.Limit{perago.PerHour(1).By(strconv.FormatInt(b.User.ID, 10))}
})
```

`By` segments the limit: here every user gets one backup an hour, instead of one backup an hour for everyone. The limits are `PerSecond(n)`, `PerMinute(n)`, `PerMinutes(minutes, n)`, `PerHour(n)`, `PerDay(n)` and `NoLimit()`. Several limits apply together.

Attach the limiter with `RateLimited`:

```go
func (j *Backup) Middleware() []perago.Middleware {
	return []perago.Middleware{middleware.RateLimited("backups")}
}
```

A job over the limit is released until the window reopens. A release uses an attempt, so tune `Tries` and `MaxExceptions`, or use `RetryUntil` to bound the time instead of the attempts. A job's settings decide what happens when it keeps hitting the limit:

```go
func (j *Backup) RetryUntil() time.Time { return time.Now().Add(12 * time.Hour) }
```

To choose the release delay, or to delete limited jobs instead of retrying them:

```go
middleware.RateLimited("backups").ReleaseAfter(time.Minute)
middleware.RateLimited("backups").DontRelease()
```

A `RateLimited` that names a limiter nobody registered lets the job through.

### Rate limiting with Redis

`RateLimitedWithRedis` keeps the counters in a Redis cache, whatever the Manager's cache is. It takes the cache instead of a connection name:

```go
middleware.RateLimitedWithRedis("backups", redis.NewCache(limiterRedis, ""))
// same as
middleware.RateLimited("backups").Via(redis.NewCache(limiterRedis, ""))
```

## Preventing job overlaps

`WithoutOverlapping` lets only one job with a key run at a time. Updating a user's credit score from two workers at once can lose an update; with the user ID as key, the second job waits:

```go
func (j *UpdateCreditScore) Middleware() []perago.Middleware {
	return []perago.Middleware{middleware.WithoutOverlapping(j.UserID)}
}
```

The key can be any value; it is formatted with `fmt.Sprint`. An overlapping job is released back to the queue right away. As with every release, that uses an attempt, and the default of one attempt means it fails. Give the job `Tries`, or choose what happens:

```go
middleware.WithoutOverlapping(j.UserID).ReleaseAfter(time.Minute)    // retry in a minute
middleware.WithoutOverlapping(j.UserID).DontRelease()                // delete overlapping jobs
middleware.WithoutOverlapping(j.UserID).ExpireAfter(3 * time.Minute) // free the lock if a worker dies
```

The lock is released when the job returns. A worker that crashes mid-job leaves it held; `ExpireAfter` gives it a lifetime so the key is not blocked forever. Pick something longer than the job's timeout.

The lock needs an atomic cache: memory (one process), database or Redis.

### Sharing lock keys across job classes

By default the key is scoped to the job type, so a `ProviderIsDown` and a `ProviderIsUp` for the same provider can run at the same time. `Shared` drops the type from the key:

```go
func (j *ProviderIsDown) Middleware() []perago.Middleware {
	return []perago.Middleware{middleware.WithoutOverlapping("status:" + j.Provider).Shared()}
}

func (j *ProviderIsUp) Middleware() []perago.Middleware {
	return []perago.Middleware{middleware.WithoutOverlapping("status:" + j.Provider).Shared()}
}
```

## Throttling exceptions

When a job calls an unstable service, retrying it as fast as the queue allows makes things worse. `ThrottlesExceptions` lets the job return a number of errors, then holds it back:

```go
func (j *SyncInvoices) Middleware() []perago.Middleware {
	return []perago.Middleware{middleware.ThrottlesExceptions(10, 5*time.Minute)}
}

func (j *SyncInvoices) RetryUntil() time.Time { return time.Now().Add(30 * time.Minute) }
```

After 10 errors, the job is released without running for the next 5 minutes. A success resets the count. Every throttled release uses an attempt, so pair the middleware with `RetryUntil` rather than `Tries`.

Errors below the threshold release the job right away. `Backoff` delays those retries, and `BackoffFunc` computes the delay from the error:

```go
middleware.ThrottlesExceptions(10, 5*time.Minute).Backoff(5 * time.Second)

middleware.ThrottlesExceptions(10, 5*time.Minute).BackoffFunc(func(err error) time.Duration {
	var rl *api.RateLimitedError
	if errors.As(err, &rl) {
		return rl.RetryAfter
	}
	return 5 * time.Second
})
```

The count is kept per job type. `By` shares one count between jobs that call the same service:

```go
middleware.ThrottlesExceptions(10, 10*time.Minute).By("payment-provider")
```

By default every error counts. `When` throttles only some, and returns the others as usual errors:

```go
middleware.ThrottlesExceptions(10, 10*time.Minute).
	When(func(err error) bool { return errors.Is(err, ErrHTTPTimeout) })
```

`DeleteWhen` deletes the job, instead of releasing it, on errors that will never go away. `DeleteWhenFunc` takes a predicate:

```go
middleware.ThrottlesExceptions(2, 10*time.Minute).DeleteWhen(ErrCustomerDeleted)
```

Throttled errors are swallowed, so they never reach `JobExceptionOccurred` or your error reporting. `Report` sends them to a reporter of your choice, optionally filtered:

```go
middleware.ThrottlesExceptions(10, 10*time.Minute).Report(func(ctx context.Context, err error) {
	sentry.CaptureException(err)
}, func(err error) bool { return !errors.Is(err, context.Canceled) })
```

### Throttling exceptions with Redis

```go
middleware.ThrottlesExceptionsWithRedis(10, 10*time.Minute, redis.NewCache(limiterRedis, ""))
// same as
middleware.ThrottlesExceptions(10, 10*time.Minute).Via(redis.NewCache(limiterRedis, ""))
```

## Releasing jobs

`ReleaseWhen` and `ReleaseUnless` (Laravel's `Release::when` and `Release::unless`) put a job back on the queue, without running it, depending on a condition:

```go
func (j *SendNewsletter) Middleware() []perago.Middleware {
	return []perago.Middleware{
		middleware.ReleaseWhen(middleware.Always(j.Paused), time.Minute),
		middleware.ReleaseUnless(func(ctx context.Context, job perago.Job) bool {
			return mailer.Healthy(ctx) // evaluated when the job runs
		}, 30*time.Second),
	}
}
```

`middleware.Always(v)` is a fixed condition, decided when `Middleware` is called on the worker. A `middleware.Condition` function is evaluated each time the job comes up. Releases use attempts here too.

## Skipping jobs

`SkipWhen` and `SkipUnless` (`Skip::when`, `Skip::unless`) delete the job without running it:

```go
func (j *SendReminder) Middleware() []perago.Middleware {
	return []perago.Middleware{
		middleware.SkipWhen(func(ctx context.Context, job perago.Job) bool {
			return j.shouldSkip(ctx)
		}),
	}
}
```

`SkipIfBatchCancelled` skips a batched job whose batch was cancelled. See [Batches](batches.md#cancelling-batches).

## Failing on specific errors

`FailOnException` retries errors as usual but fails the job at once, with no more attempts, on the errors you list (matched with `errors.Is`). `FailOnExceptionFunc` takes a predicate:

```go
func (j *SyncChatHistory) Tries() int { return 3 }

func (j *SyncChatHistory) Middleware() []perago.Middleware {
	return []perago.Middleware{middleware.FailOnException(ErrUnauthorized)}
}
```

A network blip is retried up to three times; a revoked token fails the job immediately. For rules that apply to every job, see [stopping retries by error](retries.md#stopping-retries-by-error).

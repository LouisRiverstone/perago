---
title: Job middleware
nav_order: 5
description: "RateLimited, WithoutOverlapping, ThrottlesExceptions, FailOnException, Skip, Release and custom middleware."
---

# Job middleware

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

Middleware wraps `Handle`. Return them from the job's `Middleware` method; they run in order:

```go
func (j *Backup) Middleware() []perago.Middleware {
	return []perago.Middleware{
		middleware.RateLimited("backups"),
		middleware.WithoutOverlapping(j.UserID),
	}
}
```

Write your own with `perago make:job-middleware Name`, or with `perago.MiddlewareFunc`:

```go
perago.MiddlewareFunc(func(ctx context.Context, job perago.Job, next perago.Next) error {
	if !redisUp() {
		rt, _ := perago.JobFrom(ctx)
		rt.Release(time.Minute)
		return nil
	}
	return next(ctx)
})
```

Middleware state (counters, locks) lives in the Manager's `Cache` unless `Via(cache)` points it elsewhere. Passing a `redis.Cache` gives the `*WithRedis` variants, and so do `RateLimitedWithRedis` and `ThrottlesExceptionsWithRedis`.

## Rate limiting

```go
m.RateLimiter("backups", func(job perago.Job) []perago.Limit {
	b := job.(*Backup)
	if b.Premium {
		return []perago.Limit{perago.NoLimit()}
	}
	return []perago.Limit{perago.PerHour(1).By(strconv.Itoa(b.UserID))}
})

middleware.RateLimited("backups")                         // release until the window reopens
middleware.RateLimited("backups").ReleaseAfter(time.Minute)
middleware.RateLimited("backups").DontRelease()           // delete instead
```

Limits: `PerSecond`, `PerMinute`, `PerMinutes(n, max)`, `PerHour`, `PerDay`, `NoLimit`, each with `.By(key)`. A released job still uses an attempt, so pair rate limits with `Tries() int { return 0 }` or `RetryUntil`.

## Preventing overlaps

```go
middleware.WithoutOverlapping(j.UserID)                  // release overlapping jobs right away
middleware.WithoutOverlapping(j.UserID).ReleaseAfter(time.Minute)
middleware.WithoutOverlapping(j.UserID).DontRelease()    // delete them
middleware.WithoutOverlapping(j.UserID).ExpireAfter(3 * time.Minute) // free a stuck lock
middleware.WithoutOverlapping(j.UserID).Shared()         // one lock across job types
```

## Throttling exceptions

```go
middleware.ThrottlesExceptions(10, 5*time.Minute). // after 10 errors, wait 5 minutes
	Backoff(5 * time.Second).                         // or BackoffFunc(func(err error) time.Duration)
	By("payment-provider").                           // share the throttle across jobs
	When(func(err error) bool { return errors.Is(err, ErrHTTP) }). // only throttle these
	DeleteWhen(ErrGone).                              // delete the job on these
	Report(logError, nil)                             // report throttled errors
```

Combine it with `RetryUntil` rather than `Tries`.

## Failing on specific errors

```go
middleware.FailOnException(ErrUnauthorized)   // fail at once on these; retry the rest
middleware.FailOnExceptionFunc(isPermanent)
```

## Releasing and skipping

```go
middleware.ReleaseWhen(middleware.Always(maintenance), time.Minute) // Release::when
middleware.ReleaseUnless(func(ctx context.Context, job perago.Job) bool { return ready() }, time.Minute)
middleware.SkipWhen(middleware.Always(j.Cancelled))                 // Skip::when: delete, don't run
middleware.SkipUnless(func(ctx context.Context, job perago.Job) bool { return enabled() })
middleware.SkipIfBatchCancelled()
```

// Package middleware has Laravel's job middleware: RateLimited, WithoutOverlapping,
// ThrottlesExceptions, Release, Skip and SkipIfBatchCancelled.
//
//	func (j *Backup) Middleware() []perago.Middleware {
//		return []perago.Middleware{
//			middleware.RateLimited("backups"),
//			middleware.WithoutOverlapping(j.UserID).ReleaseAfter(time.Minute),
//		}
//	}
//
// Every middleware stores its state in the Manager's Cache unless Via points it elsewhere
// (a redis.Cache gives Laravel's *WithRedis variants).
package middleware

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/LouisRiverstone/perago"
)

func runtimeOf(ctx context.Context) (*perago.Runtime, error) {
	rt, ok := perago.JobFrom(ctx)
	if !ok {
		return nil, perago.ErrNotInJob
	}
	return rt, nil
}

func cacheOf(rt *perago.Runtime, via perago.Cache) perago.Cache {
	if via != nil {
		return via
	}
	return rt.Manager().Cache()
}

func hits(rt *perago.Runtime, via perago.Cache) perago.Hits {
	return perago.Hits{Cache: cacheOf(rt, via), Now: rt.Manager().Now}
}

func hash(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:16])
}

// RateLimitedMiddleware releases jobs that exceed a named limiter (RateLimited).
type RateLimitedMiddleware struct {
	name    string
	release *time.Duration
	dontRel bool
	via     perago.Cache
}

// RateLimited applies the limiter registered with Manager.RateLimiter(name, ...). Jobs over the
// limit are released until the window reopens.
func RateLimited(name string) *RateLimitedMiddleware { return &RateLimitedMiddleware{name: name} }

// RateLimitedWithRedis is RateLimited counting in a Redis cache.
func RateLimitedWithRedis(name string, cache perago.Cache) *RateLimitedMiddleware {
	return RateLimited(name).Via(cache)
}

// ReleaseAfter releases limited jobs after d instead of when the window reopens.
func (m *RateLimitedMiddleware) ReleaseAfter(d time.Duration) *RateLimitedMiddleware {
	m.release = &d
	return m
}

// DontRelease deletes limited jobs instead of releasing them.
func (m *RateLimitedMiddleware) DontRelease() *RateLimitedMiddleware { m.dontRel = true; return m }

// Via counts hits in c instead of the Manager's cache.
func (m *RateLimitedMiddleware) Via(c perago.Cache) *RateLimitedMiddleware { m.via = c; return m }

func (m *RateLimitedMiddleware) Handle(ctx context.Context, job perago.Job, next perago.Next) error {
	rt, err := runtimeOf(ctx)
	if err != nil {
		return err
	}
	limiter, ok := rt.Manager().Limiter(m.name)
	if !ok {
		return next(ctx)
	}
	h := hits(rt, m.via)
	for _, l := range limiter(job) {
		if l.Unlimited || l.MaxAttempts <= 0 {
			continue
		}
		key := "perago:limiter:" + hash(m.name+"|"+l.Key)
		over, err := h.TooManyAttempts(ctx, key, l.MaxAttempts)
		if err != nil {
			return err
		}
		if over {
			return m.limited(ctx, rt, h, key)
		}
		if _, err := h.Hit(ctx, key, l.Decay); err != nil {
			return err
		}
	}
	return next(ctx)
}

func (m *RateLimitedMiddleware) limited(ctx context.Context, rt *perago.Runtime, h perago.Hits, key string) error {
	if m.dontRel {
		rt.Delete()
		return nil
	}
	if m.release != nil {
		rt.Release(*m.release)
		return nil
	}
	wait, err := h.AvailableIn(ctx, key)
	if err != nil {
		return err
	}
	rt.Release(wait)
	return nil
}

// OverlapMiddleware lets only one job with a key run at a time (WithoutOverlapping).
type OverlapMiddleware struct {
	key     string
	release *time.Duration
	expire  time.Duration
	shared  bool
	via     perago.Cache
}

// WithoutOverlapping holds an atomic lock on key while the job runs. Overlapping jobs are released
// right away, or after ReleaseAfter, or deleted with DontRelease.
func WithoutOverlapping(key any) *OverlapMiddleware {
	zero := time.Duration(0)
	return &OverlapMiddleware{key: fmt.Sprint(key), release: &zero}
}

// ReleaseAfter re-queues overlapping jobs after d.
func (m *OverlapMiddleware) ReleaseAfter(d time.Duration) *OverlapMiddleware {
	m.release = &d
	return m
}

// DontRelease deletes overlapping jobs.
func (m *OverlapMiddleware) DontRelease() *OverlapMiddleware { m.release = nil; return m }

// ExpireAfter frees the lock after d even if the job never finished (a crashed worker).
func (m *OverlapMiddleware) ExpireAfter(d time.Duration) *OverlapMiddleware { m.expire = d; return m }

// Shared shares the lock with every job class using the same key.
func (m *OverlapMiddleware) Shared() *OverlapMiddleware { m.shared = true; return m }

// Via keeps the lock in c instead of the Manager's cache.
func (m *OverlapMiddleware) Via(c perago.Cache) *OverlapMiddleware { m.via = c; return m }

// Key is the lock key for job.
func (m *OverlapMiddleware) Key(job perago.Job) string {
	if m.shared {
		return "perago:overlap:" + m.key
	}
	return "perago:overlap:" + perago.NameOf(job) + ":" + m.key
}

func (m *OverlapMiddleware) Handle(ctx context.Context, job perago.Job, next perago.Next) error {
	rt, err := runtimeOf(ctx)
	if err != nil {
		return err
	}
	lock := perago.NewLock(cacheOf(rt, m.via), m.Key(job), rt.UUID(), m.expire)
	ok, err := lock.Get(ctx)
	if err != nil {
		return err
	}
	if !ok {
		if m.release == nil {
			rt.Delete()
			return nil
		}
		rt.Release(*m.release)
		return nil
	}
	defer func() { _, _ = lock.Release(context.WithoutCancel(ctx)) }()
	return next(ctx)
}

// ThrottleMiddleware backs off a job whose dependency keeps failing (ThrottlesExceptions).
type ThrottleMiddleware struct {
	maxAttempts int
	decay       time.Duration
	backoff     func(err error) time.Duration
	key         string
	when        func(err error) bool
	deleteWhen  func(err error) bool
	report      func(err error) bool
	reporter    func(ctx context.Context, err error)
	via         perago.Cache
}

// ThrottlesExceptions lets a job throw maxAttempts errors; then, for decay, the job is released
// without running. Pair it with RetryUntil rather than Tries.
func ThrottlesExceptions(maxAttempts int, decay time.Duration) *ThrottleMiddleware {
	return &ThrottleMiddleware{maxAttempts: maxAttempts, decay: decay}
}

// ThrottlesExceptionsWithRedis is ThrottlesExceptions counting in a Redis cache.
func ThrottlesExceptionsWithRedis(maxAttempts int, decay time.Duration, cache perago.Cache) *ThrottleMiddleware {
	return ThrottlesExceptions(maxAttempts, decay).Via(cache)
}

// Backoff releases a job that failed (before the throttle opens) after d.
func (m *ThrottleMiddleware) Backoff(d time.Duration) *ThrottleMiddleware {
	m.backoff = func(error) time.Duration { return d }
	return m
}

// BackoffFunc computes the release delay from the error.
func (m *ThrottleMiddleware) BackoffFunc(fn func(err error) time.Duration) *ThrottleMiddleware {
	m.backoff = fn
	return m
}

// By shares one throttle between every job using key.
func (m *ThrottleMiddleware) By(key string) *ThrottleMiddleware { m.key = key; return m }

// When throttles only errors for which fn returns true; others are returned as-is.
func (m *ThrottleMiddleware) When(fn func(err error) bool) *ThrottleMiddleware { m.when = fn; return m }

// DeleteWhen deletes the job when its error matches any target (errors.Is).
func (m *ThrottleMiddleware) DeleteWhen(targets ...error) *ThrottleMiddleware {
	return m.DeleteWhenFunc(func(err error) bool {
		for _, t := range targets {
			if errors.Is(err, t) {
				return true
			}
		}
		return false
	})
}

// DeleteWhenFunc deletes the job when fn returns true for its error.
func (m *ThrottleMiddleware) DeleteWhenFunc(fn func(err error) bool) *ThrottleMiddleware {
	m.deleteWhen = fn
	return m
}

// Report passes throttled errors for which filter returns true (all when nil) to reporter.
func (m *ThrottleMiddleware) Report(reporter func(ctx context.Context, err error), filter func(err error) bool) *ThrottleMiddleware {
	m.reporter, m.report = reporter, filter
	return m
}

// Via counts errors in c instead of the Manager's cache.
func (m *ThrottleMiddleware) Via(c perago.Cache) *ThrottleMiddleware { m.via = c; return m }

func (m *ThrottleMiddleware) Handle(ctx context.Context, job perago.Job, next perago.Next) error {
	rt, err := runtimeOf(ctx)
	if err != nil {
		return err
	}
	h := hits(rt, m.via)
	key := m.key
	if key == "" {
		key = perago.NameOf(job)
	}
	key = "perago:throttle:" + hash(key)
	over, err := h.TooManyAttempts(ctx, key, m.maxAttempts)
	if err != nil {
		return err
	}
	if over {
		wait, err := h.AvailableIn(ctx, key)
		if err != nil {
			return err
		}
		rt.Release(wait)
		return nil
	}
	jobErr := next(ctx)
	if jobErr == nil {
		return h.Clear(ctx, key)
	}
	if m.when != nil && !m.when(jobErr) {
		return jobErr
	}
	if m.deleteWhen != nil && m.deleteWhen(jobErr) {
		rt.Delete()
		return nil
	}
	if _, err := h.Hit(ctx, key, m.decay); err != nil {
		return err
	}
	if m.reporter != nil && (m.report == nil || m.report(jobErr)) {
		m.reporter(ctx, jobErr)
	}
	var delay time.Duration
	if m.backoff != nil {
		delay = m.backoff(jobErr)
	}
	rt.Release(delay)
	return nil
}

// Condition decides at run time; ReleaseWhen and SkipWhen accept it.
type Condition func(ctx context.Context, job perago.Job) bool

// Always is a Condition with a fixed answer.
func Always(v bool) Condition { return func(context.Context, perago.Job) bool { return v } }

type releaseMiddleware struct {
	cond  Condition
	want  bool
	after time.Duration
}

// ReleaseWhen releases the job after delay, without running it, when cond is true (Release::when).
func ReleaseWhen(cond Condition, after time.Duration) perago.Middleware {
	return releaseMiddleware{cond: cond, want: true, after: after}
}

// ReleaseUnless releases the job when cond is false (Release::unless).
func ReleaseUnless(cond Condition, after time.Duration) perago.Middleware {
	return releaseMiddleware{cond: cond, want: false, after: after}
}

func (m releaseMiddleware) Handle(ctx context.Context, job perago.Job, next perago.Next) error {
	if m.cond(ctx, job) != m.want {
		return next(ctx)
	}
	rt, err := runtimeOf(ctx)
	if err != nil {
		return err
	}
	rt.Release(m.after)
	return nil
}

type skipMiddleware struct {
	cond Condition
	want bool
}

// SkipWhen deletes the job without running it when cond is true (Skip::when).
func SkipWhen(cond Condition) perago.Middleware { return skipMiddleware{cond: cond, want: true} }

// SkipUnless deletes the job when cond is false (Skip::unless).
func SkipUnless(cond Condition) perago.Middleware { return skipMiddleware{cond: cond, want: false} }

func (m skipMiddleware) Handle(ctx context.Context, job perago.Job, next perago.Next) error {
	if m.cond(ctx, job) != m.want {
		return next(ctx)
	}
	rt, err := runtimeOf(ctx)
	if err != nil {
		return err
	}
	rt.Delete()
	return nil
}

// SkipIfBatchCancelled deletes batched jobs whose batch was cancelled.
func SkipIfBatchCancelled() perago.Middleware {
	return perago.MiddlewareFunc(func(ctx context.Context, job perago.Job, next perago.Next) error {
		rt, err := runtimeOf(ctx)
		if err != nil {
			return err
		}
		b, err := rt.Batch(ctx)
		if err != nil {
			return err
		}
		if b != nil && b.Cancelled() {
			rt.Delete()
			return nil
		}
		return next(ctx)
	})
}

// FailOnException fails the job at once, without further retries, when its error matches any
// target (errors.Is); other errors are retried as usual.
func FailOnException(targets ...error) perago.Middleware {
	return FailOnExceptionFunc(func(err error) bool {
		for _, t := range targets {
			if errors.Is(err, t) {
				return true
			}
		}
		return false
	})
}

// FailOnExceptionFunc is FailOnException with a predicate.
func FailOnExceptionFunc(match func(err error) bool) perago.Middleware {
	return perago.MiddlewareFunc(func(ctx context.Context, job perago.Job, next perago.Next) error {
		err := next(ctx)
		if err == nil || !match(err) {
			return err
		}
		rt, rerr := runtimeOf(ctx)
		if rerr != nil {
			return err
		}
		rt.Fail(err)
		return nil
	})
}

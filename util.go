package perago

import (
	"context"
	"encoding/json"
	"strconv"
	"time"
)

func formatInt(n int64) string { return strconv.FormatInt(n, 10) }

func parseInt(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func jsonUnmarshal(data []byte, v any) error {
	if err := json.Unmarshal(data, v); err != nil {
		return Wrap(PayloadInvalid, err, "decode job data")
	}
	return nil
}

// Next calls the rest of the middleware pipeline and, at its end, the job.
type Next func(ctx context.Context) error

// Middleware wraps a job's Handle (job middleware). Use JobFrom(ctx) to release or delete the job.
type Middleware interface {
	Handle(ctx context.Context, job Job, next Next) error
}

// MiddlewareFunc adapts a function to Middleware.
type MiddlewareFunc func(ctx context.Context, job Job, next Next) error

func (f MiddlewareFunc) Handle(ctx context.Context, job Job, next Next) error {
	return f(ctx, job, next)
}

// Limit is one rate limit (Laravel's Limit::perMinute(...)->by(...)).
type Limit struct {
	MaxAttempts int
	Decay       time.Duration
	Key         string
	Unlimited   bool
}

func PerSecond(n int) Limit { return Limit{MaxAttempts: n, Decay: time.Second} }
func PerMinute(n int) Limit { return Limit{MaxAttempts: n, Decay: time.Minute} }
func PerMinutes(minutes, n int) Limit {
	return Limit{MaxAttempts: n, Decay: time.Duration(minutes) * time.Minute}
}
func PerHour(n int) Limit { return Limit{MaxAttempts: n, Decay: time.Hour} }
func PerDay(n int) Limit  { return Limit{MaxAttempts: n, Decay: 24 * time.Hour} }

// NoLimit is Limit::none().
func NoLimit() Limit { return Limit{Unlimited: true} }

// By segments the limit per key (a user id, a tenant, ...).
func (l Limit) By(key string) Limit { l.Key = key; return l }

// RateLimiter defines a named limiter for middleware.RateLimited (RateLimiter::for).
func (m *Manager) RateLimiter(name string, fn func(job Job) []Limit) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.limiters[name] = fn
}

// Limiter returns the named limiter.
func (m *Manager) Limiter(name string) (func(job Job) []Limit, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	fn, ok := m.limiters[name]
	return fn, ok
}

// Hits is a cache-backed hit counter with a decay window (Laravel's RateLimiter).
type Hits struct {
	Cache Cache
	Now   func() time.Time
}

func (h Hits) now() time.Time {
	if h.Now == nil {
		return time.Now()
	}
	return h.Now()
}

// TooManyAttempts reports whether key has used max hits in its current window.
func (h Hits) TooManyAttempts(ctx context.Context, key string, max int) (bool, error) {
	v, ok, err := h.Cache.Get(ctx, key)
	if err != nil {
		return false, err
	}
	if !ok || int(parseInt(v)) < max {
		return false, nil
	}
	_, timer, err := h.Cache.Get(ctx, key+":timer")
	if err != nil {
		return false, err
	}
	if timer {
		return true, nil
	}
	return false, h.Cache.Forget(ctx, key)
}

// Hit counts one hit, opening a window of decay when there is none.
func (h Hits) Hit(ctx context.Context, key string, decay time.Duration) (int64, error) {
	if _, err := h.Cache.Add(ctx, key+":timer", formatInt(h.now().Add(decay).Unix()), decay); err != nil {
		return 0, err
	}
	if _, err := h.Cache.Add(ctx, key, "0", decay); err != nil {
		return 0, err
	}
	return h.Cache.Increment(ctx, key, 1, decay)
}

// AvailableIn is how long until the window of key closes.
func (h Hits) AvailableIn(ctx context.Context, key string) (time.Duration, error) {
	v, ok, err := h.Cache.Get(ctx, key+":timer")
	if err != nil || !ok {
		return 0, err
	}
	return max(time.Unix(parseInt(v), 0).Sub(h.now()), 0), nil
}

// Clear resets key.
func (h Hits) Clear(ctx context.Context, key string) error {
	if err := h.Cache.Forget(ctx, key); err != nil {
		return err
	}
	return h.Cache.Forget(ctx, key+":timer")
}

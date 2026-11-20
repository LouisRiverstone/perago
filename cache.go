package perago

import (
	"context"
	"strconv"
	"sync"
	"time"
)

// Cache is the atomic key/value store behind unique jobs, debouncing, WithoutOverlapping,
// rate limiting, queue:restart and queue:pause. A ttl of 0 never expires.
// Implementations: NewMemoryCache (one process), database.NewCache and redis.NewCache.
type Cache interface {
	Get(ctx context.Context, key string) (value string, ok bool, err error)
	Put(ctx context.Context, key, value string, ttl time.Duration) error
	// Add stores value only when key is absent or expired, atomically, and reports whether it did.
	Add(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	// Increment adds by to the integer at key, creating it with ttl when absent.
	Increment(ctx context.Context, key string, by int64, ttl time.Duration) (int64, error)
	Forget(ctx context.Context, key string) error
	// ForgetIf deletes key only while it holds value (releasing a lock its owner holds).
	ForgetIf(ctx context.Context, key, value string) (bool, error)
}

// Lock is an atomic lock on a Cache (Laravel's Cache::lock).
type Lock struct {
	cache Cache
	key   string
	owner string
	ttl   time.Duration
}

// NewLock prepares a lock; owner identifies the holder (an empty owner gets a random one).
func NewLock(c Cache, key, owner string, ttl time.Duration) *Lock {
	if owner == "" {
		owner = newUUID()
	}
	return &Lock{cache: c, key: key, owner: owner, ttl: ttl}
}

// Get acquires the lock without waiting.
func (l *Lock) Get(ctx context.Context) (bool, error) { return l.cache.Add(ctx, l.key, l.owner, l.ttl) }

// Release frees the lock when this owner still holds it.
func (l *Lock) Release(ctx context.Context) (bool, error) {
	return l.cache.ForgetIf(ctx, l.key, l.owner)
}

// ForceRelease frees the lock whoever holds it.
func (l *Lock) ForceRelease(ctx context.Context) error { return l.cache.Forget(ctx, l.key) }

// Owner is the lock owner token.
func (l *Lock) Owner() string { return l.owner }

type memoryEntry struct {
	value   string
	expires time.Time
}

// MemoryCache is a Cache for a single process (tests, sync/background connections).
type MemoryCache struct {
	mu  sync.Mutex
	m   map[string]memoryEntry
	now func() time.Time
}

// NewMemoryCache returns an empty in-process cache.
func NewMemoryCache() *MemoryCache { return &MemoryCache{m: map[string]memoryEntry{}, now: time.Now} }

func (c *MemoryCache) live(key string) (memoryEntry, bool) {
	e, ok := c.m[key]
	if !ok {
		return e, false
	}
	if !e.expires.IsZero() && !c.now().Before(e.expires) {
		delete(c.m, key)
		return e, false
	}
	return e, true
}

func (c *MemoryCache) expiry(ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return c.now().Add(ttl)
}

func (c *MemoryCache) Get(_ context.Context, key string) (string, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.live(key)
	return e.value, ok, nil
}

func (c *MemoryCache) Put(_ context.Context, key, value string, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[key] = memoryEntry{value: value, expires: c.expiry(ttl)}
	return nil
}

func (c *MemoryCache) Add(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.live(key); ok {
		return false, nil
	}
	c.m[key] = memoryEntry{value: value, expires: c.expiry(ttl)}
	return true, nil
}

func (c *MemoryCache) Increment(_ context.Context, key string, by int64, ttl time.Duration) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.live(key)
	if !ok {
		e = memoryEntry{value: "0", expires: c.expiry(ttl)}
	}
	n, err := strconv.ParseInt(e.value, 10, 64)
	if err != nil {
		return 0, Wrap(CacheFailed, err, "increment %q: not an integer", key)
	}
	n += by
	e.value = strconv.FormatInt(n, 10)
	c.m[key] = e
	return n, nil
}

func (c *MemoryCache) Forget(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, key)
	return nil
}

func (c *MemoryCache) ForgetIf(_ context.Context, key, value string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.live(key)
	if !ok || e.value != value {
		return false, nil
	}
	delete(c.m, key)
	return true, nil
}

// SetClock replaces the cache's clock (tests).
func (c *MemoryCache) SetClock(now func() time.Time) { c.now = now }

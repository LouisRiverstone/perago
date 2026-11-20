// Package perago is Laravel's queue system for Go: jobs, dispatching, delays, retries,
// middleware, unique and encrypted jobs, chains, batches, failed jobs, workers and test fakes,
// over the database (every migro dialect), Redis, SQS and Beanstalkd.
//
//	m, _ := perago.New(perago.Config{
//		Default: "database",
//		Connections: map[string]perago.Connection{
//			"database": {Driver: database.NewDriver(db, database.DriverOptions{})},
//			"sync":     {Driver: perago.SyncDriver()},
//		},
//		Failed:  database.NewFailedJobStore(db, ""),
//		Batches: database.NewBatchStore(db, ""),
//		Cache:   database.NewCache(db, ""),
//	})
//	_, err := m.Dispatch(&ProcessPodcast{PodcastID: 1}).OnQueue("podcasts").Send(ctx)
package perago

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Connection is one entry of Laravel's config/queue.php connections.
type Connection struct {
	Driver Driver
	// Queue is the default queue name ("default").
	Queue string
	// AfterCommit holds every dispatch made inside a Manager.Transaction until it commits.
	AfterCommit bool
}

// Config configures New.
type Config struct {
	// Default names the connection used when a job does not pick one. Defaults to the
	// PERAGO_CONNECTION or QUEUE_CONNECTION environment variable, then "sync".
	Default     string
	Connections map[string]Connection

	// Failed stores failed jobs; nil disables failed job storage (Laravel's "null" driver).
	Failed FailedJobStore
	// Batches stores job batches; nil makes Batch().Dispatch fail with INVALID_CONFIG.
	Batches BatchStore
	// Cache backs unique jobs, debouncing, overlap locks, rate limiters, queue:restart and
	// queue:pause. nil uses an in-process MemoryCache, enough for a single process only.
	Cache Cache
	// Key is the 32-byte AES-256 key for ShouldBeEncrypted jobs.
	Key []byte
	// PreviousKeys still decrypt jobs queued before a key rotation (APP_PREVIOUS_KEYS).
	PreviousKeys [][]byte

	Logger *slog.Logger
	Now    func() time.Time
}

type route struct {
	match      func(Job) bool
	connection string
	queue      string
}

type forward struct{ queue, connection string }

// Manager is the queue manager and dispatcher (Laravel's Queue and Bus facades).
type Manager struct {
	cfg     Config
	conns   map[string]Connection
	def     string
	cache   Cache
	failed  FailedJobStore
	batches BatchStore
	log     *slog.Logger
	now     func() time.Time

	events eventBus

	mu       sync.RWMutex
	routes   []route
	forwards map[string]forward
	limiters map[string]func(job Job) []Limit
	noRetry  []func(error) bool
	fake     Interceptor

	localMu sync.Mutex
	local   map[string]func(ctx context.Context) error

	bg        sync.WaitGroup
	noPolling atomic.Bool
}

// New validates cfg and builds a Manager.
func New(cfg Config) (*Manager, error) {
	if len(cfg.Connections) == 0 {
		cfg.Connections = map[string]Connection{"sync": {Driver: SyncDriver()}}
	}
	def := cfg.Default
	for _, k := range []string{"PERAGO_CONNECTION", "QUEUE_CONNECTION"} {
		if def == "" {
			def = os.Getenv(k)
		}
	}
	if def == "" {
		def = "sync"
	}
	conns := make(map[string]Connection, len(cfg.Connections))
	for name, c := range cfg.Connections {
		if c.Driver == nil {
			return nil, Newf(InvalidConfig, "connection %q has no driver", name)
		}
		if c.Queue == "" {
			c.Queue = "default"
		}
		conns[name] = c
	}
	if _, ok := conns[def]; !ok {
		return nil, Newf(ConnectionNotFound, "default connection %q is not configured", def)
	}
	for name, c := range conns {
		d, ok := c.Driver.(localDriver)
		if !ok || d.kind != kindFailover {
			continue
		}
		if len(d.conns) == 0 {
			return nil, Newf(InvalidConfig, "failover connection %q lists no connections", name)
		}
		for _, n := range d.conns {
			if _, ok := conns[n]; !ok || n == name {
				return nil, Newf(InvalidConfig, "failover connection %q lists unknown connection %q", name, n)
			}
		}
	}
	m := &Manager{
		cfg: cfg, conns: conns, def: def,
		cache: cfg.Cache, failed: cfg.Failed, batches: cfg.Batches,
		log: cfg.Logger, now: cfg.Now,
		forwards: map[string]forward{}, limiters: map[string]func(Job) []Limit{},
		local: map[string]func(context.Context) error{},
	}
	if m.cache == nil {
		m.cache = NewMemoryCache()
	}
	if m.failed == nil {
		m.failed = NullFailedJobStore{}
	}
	if m.log == nil {
		m.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if m.now == nil {
		m.now = time.Now
	}
	return m, nil
}

// DefaultConnection is the name of the default connection.
func (m *Manager) DefaultConnection() string { return m.def }

// Connections lists the configured connection names, sorted.
func (m *Manager) Connections() []string {
	out := make([]string, 0, len(m.conns))
	for n := range m.conns {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Connection returns the configuration of a named connection ("" is the default).
func (m *Manager) Connection(name string) (Connection, error) {
	if name == "" {
		name = m.def
	}
	c, ok := m.conns[name]
	if !ok {
		return Connection{}, Newf(ConnectionNotFound, "queue connection %q is not configured", name)
	}
	return c, nil
}

// Cache is the cache the manager uses for locks and signals.
func (m *Manager) Cache() Cache { return m.cache }

// FailedJobs is the failed job store.
func (m *Manager) FailedJobs() FailedJobStore { return m.failed }

// BatchStore is the batch store, nil when batching is not configured.
func (m *Manager) BatchStore() BatchStore { return m.batches }

// Logger is the manager's logger.
func (m *Manager) Logger() *slog.Logger { return m.log }

// Now is the manager's clock.
func (m *Manager) Now() time.Time { return m.now() }

// Route sends every job of prototype's type to connection and queue unless the dispatch says
// otherwise (Queue::route). An empty connection or queue keeps the default.
func (m *Manager) Route(prototype Job, connection, queue string) {
	t := reflect.TypeOf(prototype)
	base := baseType(prototype)
	m.addRoute(func(j Job) bool {
		jt := reflect.TypeOf(j)
		return jt == t || baseType(j) == base
	}, connection, queue)
}

// RouteMatching routes every job for which match returns true; use it to route by interface,
// embedded type or anything else (Queue::route with an interface, trait or parent class).
func (m *Manager) RouteMatching(match func(Job) bool, connection, queue string) {
	m.addRoute(match, connection, queue)
}

// RouteInterface routes every job implementing I.
func RouteInterface[I any](m *Manager, connection, queue string) {
	m.addRoute(func(j Job) bool { _, ok := j.(I); return ok }, connection, queue)
}

func (m *Manager) addRoute(match func(Job) bool, connection, queue string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.routes = append(m.routes, route{match: match, connection: connection, queue: queue})
}

func (m *Manager) routeFor(j Job) (route, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for i := len(m.routes) - 1; i >= 0; i-- {
		if m.routes[i].match(j) {
			return m.routes[i], true
		}
	}
	return route{}, false
}

// Forward rewrites dispatches to queue so they go to toQueue, on connection when not empty
// (Queue::forward). A connection chosen explicitly for the job wins over the forward's.
func (m *Manager) Forward(queue, toQueue, connection string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.forwards[queue] = forward{queue: toQueue, connection: connection}
}

// DontRetry makes jobs that fail with an error matching any target (errors.Is) fail
// immediately instead of being retried ($exceptions->dontRetry).
func (m *Manager) DontRetry(targets ...error) {
	for _, t := range targets {
		m.DontRetryWhen(func(err error) bool { return errors.Is(err, t) })
	}
}

// DontRetryWhen is DontRetry with a predicate ($exceptions->dontRetryWhen).
func (m *Manager) DontRetryWhen(fn func(err error) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.noRetry = append(m.noRetry, fn)
}

func (m *Manager) shouldNotRetry(err error) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, fn := range m.noRetry {
		if fn(err) {
			return true
		}
	}
	return false
}

// Size counts the jobs of a queue ("" is the connection's default queue).
func (m *Manager) Size(ctx context.Context, connection, queue string) (int64, error) {
	c, err := m.Connection(connection)
	if err != nil {
		return 0, err
	}
	if queue == "" {
		queue = c.Queue
	}
	return c.Driver.Size(ctx, queue)
}

// Clear deletes every job of a queue (queue:clear).
func (m *Manager) Clear(ctx context.Context, connection, queue string) (int64, error) {
	c, err := m.Connection(connection)
	if err != nil {
		return 0, err
	}
	if queue == "" {
		queue = c.Queue
	}
	return c.Driver.Clear(ctx, queue)
}

// Wait blocks until every background and deferred job started by this manager returns.
func (m *Manager) Wait() { m.bg.Wait() }

// Close waits for in-process jobs and closes the drivers that implement Closer. Clients you
// passed to a driver (Redis, Beanstalkd, SQL) stay yours to close.
func (m *Manager) Close() error {
	m.Wait()
	var errs []error
	seen := map[any]bool{}
	closeIt := func(v any) {
		c, ok := v.(Closer)
		if !ok || seen[v] {
			return
		}
		seen[v] = true
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, c := range m.conns {
		if _, ok := c.Driver.(localDriver); ok {
			continue
		}
		closeIt(c.Driver)
	}
	return errors.Join(errs...)
}

const (
	restartKey = "perago:queue:restart"
	pausedKey  = "perago:queue:paused:"
)

// Restart tells every worker to exit after its current job (queue:restart).
func (m *Manager) Restart(ctx context.Context) error {
	return m.cache.Put(ctx, restartKey, formatInt(m.now().UnixNano())+"-"+newUUID(), 0)
}

// Pause stops workers from taking jobs from connection:queue until Resume, or until ttl passes
// when ttl > 0 (queue:pause).
func (m *Manager) Pause(ctx context.Context, connection, queue string, ttl time.Duration) error {
	connection, queue = m.resolveName(connection, queue)
	if err := m.cache.Put(ctx, pausedKey+connection+":"+queue, "1", ttl); err != nil {
		return err
	}
	m.events.emit(ctx, QueuePaused{Connection: connection, Queue: queue, TTL: ttl})
	return nil
}

// Resume lets workers take jobs from connection:queue again (queue:resume / queue:continue).
func (m *Manager) Resume(ctx context.Context, connection, queue string) error {
	connection, queue = m.resolveName(connection, queue)
	if err := m.cache.Forget(ctx, pausedKey+connection+":"+queue); err != nil {
		return err
	}
	m.events.emit(ctx, QueueResumed{Connection: connection, Queue: queue})
	return nil
}

// IsPaused reports whether connection:queue is paused, on its own or by PauseAll.
func (m *Manager) IsPaused(ctx context.Context, connection, queue string) (bool, error) {
	connection, queue = m.resolveName(connection, queue)
	for _, k := range []string{pausedKey + connection + ":" + queue, pausedAllKey} {
		_, ok, err := m.cache.Get(ctx, k)
		if err != nil || ok {
			return ok, err
		}
	}
	return false, nil
}

const pausedAllKey = "perago:queue:paused-all"

// PauseAll pauses every queue of every connection (queue:pause --all).
func (m *Manager) PauseAll(ctx context.Context, ttl time.Duration) error {
	if err := m.cache.Put(ctx, pausedAllKey, "1", ttl); err != nil {
		return err
	}
	m.events.emit(ctx, QueuePaused{Connection: "*", Queue: "*", TTL: ttl})
	return nil
}

// ResumeAll lifts PauseAll; queues paused one by one stay paused (queue:resume --all).
func (m *Manager) ResumeAll(ctx context.Context) error {
	if err := m.cache.Forget(ctx, pausedAllKey); err != nil {
		return err
	}
	m.events.emit(ctx, QueueResumed{Connection: "*", Queue: "*"})
	return nil
}

// WithoutInterruptionPolling makes workers skip the cache checks for queue:restart and
// queue:pause (Queue::withoutInterruptionPolling).
func (m *Manager) WithoutInterruptionPolling() { m.noPolling.Store(true) }

func (m *Manager) resolveName(connection, queue string) (string, string) {
	if connection == "" {
		connection = m.def
	}
	if queue == "" {
		if c, ok := m.conns[connection]; ok {
			queue = c.Queue
		}
	}
	return connection, queue
}

// lastRestart is the current queue:restart marker ("" when none).
func (m *Manager) lastRestart(ctx context.Context) string {
	v, _, err := m.cache.Get(ctx, restartKey)
	if err != nil {
		return ""
	}
	return v
}

func (m *Manager) storeLocal(fn func(ctx context.Context) error) string {
	id := newUUID()
	m.localMu.Lock()
	defer m.localMu.Unlock()
	m.local[id] = fn
	return id
}

// forgetLocal drops the in-process closures referenced by encoded callbacks.
func (m *Manager) forgetLocal(raws []json.RawMessage) {
	for _, raw := range raws {
		p, err := DecodePayload(raw)
		if err != nil || p.Job != closureName {
			continue
		}
		var c closureJob
		if json.Unmarshal(p.Data, &c) == nil {
			m.takeLocal(c.ID)
		}
	}
}

func (m *Manager) takeLocal(id string) func(ctx context.Context) error {
	m.localMu.Lock()
	defer m.localMu.Unlock()
	fn := m.local[id]
	delete(m.local, id)
	return fn
}

// InProcess is implemented by drivers whose jobs never leave the process (MemoryDriver), so
// they can carry closures.
type InProcess interface{ InProcess() bool }

func (m *Manager) isLocal(connection string) bool {
	c, ok := m.conns[connection]
	if !ok {
		return false
	}
	if ip, ok := c.Driver.(InProcess); ok {
		return ip.InProcess()
	}
	d, ok := c.Driver.(localDriver)
	return ok && d.kind != kindFailover
}

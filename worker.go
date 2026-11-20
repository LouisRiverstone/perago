package perago

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// Worker exit statuses (queue:work exit codes).
const (
	ExitSuccess     = 0
	ExitError       = 1
	ExitMemoryLimit = 12
)

// WorkerOptions are queue:work's flags.
type WorkerOptions struct {
	// Name identifies the worker (--name), shown in logs.
	Name string
	// Connection to work ("" is the default connection).
	Connection string
	// Queues in priority order (--queue=high,default); empty uses the connection's default queue.
	Queues []string
	// Backoff applies to jobs that declare none (--backoff=10,30); the last value repeats.
	Backoff []time.Duration
	// Tries applies to jobs that declare none (--tries). 0 means the default, 1; a negative
	// value retries forever (--tries=0).
	Tries int
	// Timeout per job (--timeout, default 60s); negative disables it.
	Timeout time.Duration
	// Grace is how long a timed-out job gets to return after its context is cancelled; when it
	// does not, the worker stops with ExitError so a supervisor can replace the process.
	// Default 5s.
	Grace time.Duration
	// Sleep between polls when every queue is empty (--sleep, default 3s).
	Sleep time.Duration
	// Rest after each job (--rest).
	Rest time.Duration
	// MemoryMB stops the worker (ExitMemoryLimit) once the process uses more (--memory=128).
	MemoryMB int
	// MemoryPercent stops it past this share of the machine's or container's memory (--memory=60%).
	MemoryPercent float64
	// MaxJobs stops the worker after processing this many jobs (--max-jobs).
	MaxJobs int
	// MaxTime stops the worker once it has run this long (--max-time).
	MaxTime time.Duration
	// Once processes a single job, or none, and returns (--once).
	Once bool
	// StopWhenEmpty returns once every queue is empty (--stop-when-empty).
	StopWhenEmpty bool
	// Force processes jobs even while Maintenance reports true (--force).
	Force bool
	// Maintenance reports whether the application is in maintenance mode.
	Maintenance func() bool
	// SkipRestartPolling and SkipPausePolling stop the worker from checking the cache for
	// queue:restart and queue:pause before each job (Worker::$restartable / $pausable = false).
	SkipRestartPolling bool
	SkipPausePolling   bool
	// HandleSignals installs SIGTERM/SIGINT/SIGQUIT (finish the job, then exit),
	// SIGUSR2 (pause) and SIGCONT (continue) handlers.
	HandleSignals bool
}

func (o *WorkerOptions) defaults(m *Manager) {
	if o.Connection == "" {
		o.Connection = m.def
	}
	if len(o.Queues) == 0 {
		o.Queues = []string{m.conns[o.Connection].Queue}
	}
	if o.Tries == 0 {
		o.Tries = 1
	}
	if o.Tries < 0 {
		o.Tries = 0
	}
	if o.Timeout == 0 {
		o.Timeout = 60 * time.Second
	}
	if o.Grace == 0 {
		o.Grace = 5 * time.Second
	}
	if o.Sleep == 0 {
		o.Sleep = 3 * time.Second
	}
}

type processOptions struct {
	maxTries  int
	backoff   []time.Duration
	timeout   time.Duration
	grace     time.Duration
	sync      bool // run once; the job error is returned and nothing is stored as failed
	logFailed bool
	leaked    *atomic.Bool
	state     *workerState
}

// Interruptible jobs are told when the worker running them receives SIGTERM, SIGINT or SIGQUIT,
// so they can stop early; the worker still waits for Handle to return.
type Interruptible interface {
	Interrupted(sig os.Signal)
}

// workerState is what signals change.
type workerState struct {
	quit    atomic.Bool
	paused  atomic.Bool
	wake    chan struct{}
	current atomic.Pointer[Runtime]
}

func (s *workerState) interrupt(sig os.Signal) {
	rt := s.current.Load()
	if rt == nil {
		return
	}
	if i, ok := rt.job.(Interruptible); ok {
		_ = safeCall(func() { i.Interrupted(sig) })
	}
}

func (s *workerState) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Work runs a worker until a stop condition (queue:work). It returns the exit status.
func (m *Manager) Work(ctx context.Context, o WorkerOptions) (int, error) {
	o.defaults(m)
	c, ok := m.conns[o.Connection]
	if !ok {
		return ExitError, Newf(ConnectionNotFound, "queue connection %q is not configured", o.Connection)
	}
	if d, ok := c.Driver.(localDriver); ok {
		return ExitError, Newf(InvalidConfig, "the %s connection %q has no worker: run one for each underlying connection", d.name(), o.Connection)
	}
	if m.noPolling.Load() {
		o.SkipRestartPolling, o.SkipPausePolling = true, true
	}
	st := &workerState{wake: make(chan struct{}, 1)}
	if o.HandleSignals {
		stop := listenSignals(st)
		defer stop()
	}
	m.events.emit(ctx, WorkerStarting{Connection: o.Connection, Queues: o.Queues, Options: o})
	start := m.now()
	restartMark := ""
	if !o.SkipRestartPolling {
		restartMark = m.lastRestart(ctx)
	}
	leaked := &atomic.Bool{}
	po := processOptions{maxTries: o.Tries, backoff: o.Backoff, timeout: o.Timeout, grace: o.Grace, logFailed: true, leaked: leaked}
	jobCtx := context.WithoutCancel(ctx)
	processed := 0
	for {
		if status, stop := m.shouldStop(ctx, st, &o, start, restartMark, processed); stop {
			m.events.emit(jobCtx, WorkerStopping{Status: status})
			return status, nil
		}
		if st.paused.Load() || (o.Maintenance != nil && o.Maintenance() && !o.Force) {
			m.sleep(ctx, st, o.Sleep)
			continue
		}
		if !m.events.emit(ctx, Looping{Connection: o.Connection, Queues: o.Queues}) {
			m.sleep(ctx, st, o.Sleep)
			continue
		}
		m.events.emit(ctx, JobPopping{Connection: o.Connection})
		r, err := m.next(ctx, c.Driver, o.Connection, o.Queues, !o.SkipPausePolling)
		if err != nil {
			m.log.Error("perago: pop failed", "connection", o.Connection, "worker", o.Name, "err", err)
			m.sleep(ctx, st, time.Second)
			continue
		}
		if r == nil {
			m.events.emit(ctx, JobPopped{Connection: o.Connection})
			if o.StopWhenEmpty || o.Once {
				m.events.emit(jobCtx, WorkerStopping{Status: ExitSuccess})
				return ExitSuccess, nil
			}
			m.events.emit(ctx, WorkerIdle{Connection: o.Connection, Queues: o.Queues})
			m.sleep(ctx, st, o.Sleep)
			continue
		}
		po.state = st
		if err := m.process(jobCtx, o.Connection, r, po); err != nil {
			m.log.Warn("perago: job error", "connection", o.Connection, "queue", r.Queue(), "worker", o.Name, "err", err)
		}
		processed++
		if leaked.Load() {
			m.events.emit(jobCtx, WorkerStopping{Status: ExitError})
			return ExitError, Newf(JobTimeout, "a job outlived its timeout and grace period; the worker must restart")
		}
		if o.Once {
			m.events.emit(jobCtx, WorkerStopping{Status: ExitSuccess})
			return ExitSuccess, nil
		}
		if o.Rest > 0 {
			m.sleep(ctx, st, o.Rest)
		}
	}
}

func (m *Manager) shouldStop(ctx context.Context, st *workerState, o *WorkerOptions, start time.Time, restartMark string, processed int) (int, bool) {
	switch {
	case ctx.Err() != nil, st.quit.Load():
		return ExitSuccess, true
	case !o.SkipRestartPolling && m.lastRestart(ctx) != restartMark:
		return ExitSuccess, true
	case memoryExceeded(o):
		return ExitMemoryLimit, true
	case o.MaxJobs > 0 && processed >= o.MaxJobs:
		return ExitSuccess, true
	case o.MaxTime > 0 && m.now().Sub(start) >= o.MaxTime:
		return ExitSuccess, true
	}
	return 0, false
}

func (m *Manager) sleep(ctx context.Context, st *workerState, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-st.wake:
	case <-t.C:
	}
}

func (m *Manager) next(ctx context.Context, d Driver, conn string, queues []string, checkPaused bool) (Reserved, error) {
	for _, q := range queues {
		if checkPaused {
			paused, err := m.IsPaused(ctx, conn, q)
			if err != nil {
				return nil, err
			}
			if paused {
				continue
			}
		}
		r, err := d.Pop(ctx, q)
		if err != nil {
			return nil, Wrap(PopFailed, err, "pop %s:%s", conn, q)
		}
		if r != nil {
			return r, nil
		}
	}
	return nil, nil
}

// process runs one reserved job through the full lifecycle (Laravel's Worker::process and
// CallQueuedHandler).
func (m *Manager) process(ctx context.Context, conn string, r Reserved, o processOptions) error {
	p, err := DecodePayload(r.Payload())
	if err != nil {
		if o.logFailed {
			m.logFailed(ctx, FailedJob{ID: r.ID(), Connection: conn, Queue: r.Queue(), Payload: r.Payload(), Exception: err.Error(), FailedAt: m.now()})
		}
		_ = r.Delete(ctx)
		return err
	}
	rt := &Runtime{m: m, connection: conn, reserved: r, payload: p, backoff: o.backoff}
	m.events.emit(ctx, JobPopped{Connection: conn, Job: rt})
	m.events.emit(ctx, JobProcessing{Connection: conn, Job: rt})
	maxTries := rt.MaxTries(o.maxTries)

	if err := m.exceededBeforeRun(ctx, rt, maxTries); err != nil {
		m.fail(ctx, rt, err, o)
		return err
	}
	job, err := m.decodeJob(ctx, p, true)
	if err != nil {
		m.fail(ctx, rt, err, o)
		return err
	}
	rt.job = job
	if err := restoreModels(ctx, job); err != nil {
		if errors.Is(err, ErrModelNotFound) && p.DeleteMissing {
			m.releaseUnique(ctx, rt)
			return r.Delete(ctx)
		}
		m.fail(ctx, rt, err, o)
		return err
	}
	if superseded, err := m.supersededByDebounce(ctx, rt); err != nil || superseded {
		return err
	}
	if p.UniqueUntilProcessing {
		m.releaseUnique(ctx, rt)
	}
	running := "perago:job-running:" + p.UUID
	if p.CountCrashes && p.MaxExceptions > 0 && !o.sync {
		if _, ok, _ := m.cache.Get(ctx, running); ok {
			_, _ = m.cache.Increment(ctx, exceptionsKey(p), 1, 24*time.Hour)
		}
		_ = m.cache.Put(ctx, running, "1", 24*time.Hour)
	}

	timeout := o.timeout
	if p.Timeout > 0 {
		timeout = time.Duration(p.Timeout) * time.Millisecond
	}
	if o.state != nil {
		o.state.current.Store(rt)
		defer o.state.current.Store(nil)
	}
	err = m.runJob(rt.bind(ctx), rt, timeout, o)
	if p.CountCrashes && p.MaxExceptions > 0 && !o.sync {
		_ = m.cache.Forget(ctx, running)
	}
	m.events.emit(ctx, JobAttempted{Connection: conn, Job: rt, ExceptionOccurred: err != nil})
	if err != nil {
		return m.handleException(ctx, rt, err, maxTries, o)
	}
	if failed, ferr := rt.HasFailed(); failed {
		m.fail(ctx, rt, ferr, o)
		if o.sync {
			return ferr
		}
		return nil
	}
	if released, delay := rt.IsReleased(); released {
		if o.sync {
			return nil
		}
		return r.Release(ctx, delay)
	}
	m.releaseUnique(ctx, rt)
	if len(p.Chain) == 0 {
		m.forgetLocal(p.ChainCatch)
	}
	if err := m.dispatchNextInChain(ctx, p); err != nil {
		m.log.Error("perago: dispatch next job in chain", "job", p.Job, "err", err)
	}
	if err := m.recordBatchSuccess(ctx, p); err != nil {
		m.log.Error("perago: record batch success", "batch", p.BatchID, "err", err)
	}
	if err := r.Delete(ctx); err != nil {
		return err
	}
	m.events.emit(ctx, JobProcessed{Connection: conn, Job: rt})
	return nil
}

func exceptionsKey(p *Payload) string { return "perago:job-exceptions:" + p.UUID }

// exceededBeforeRun fails jobs that come back after their last allowed attempt, which happens
// when a worker crashed or timed out while holding them.
func (m *Manager) exceededBeforeRun(ctx context.Context, rt *Runtime, maxTries int) error {
	p := rt.payload
	if p.RetryUntil > 0 {
		if m.now().Unix() <= p.RetryUntil {
			return nil
		}
		return Newf(MaxAttemptsExceeded, "%s has been attempted too many times (retry until %s)", p.DisplayName, time.Unix(p.RetryUntil, 0).Format(time.RFC3339))
	}
	if maxTries > 0 && rt.Attempts() > maxTries {
		return Newf(MaxAttemptsExceeded, "%s has been attempted too many times", p.DisplayName)
	}
	if p.MaxExceptions > 0 && p.CountCrashes {
		v, ok, err := m.cache.Get(ctx, exceptionsKey(p))
		if err == nil && ok && int(parseInt(v)) >= p.MaxExceptions {
			return Newf(MaxExceptionsExceeded, "%s has thrown too many exceptions", p.DisplayName)
		}
	}
	return nil
}

func (m *Manager) handleException(ctx context.Context, rt *Runtime, cause error, maxTries int, o processOptions) error {
	p := rt.payload
	m.events.emit(ctx, JobExceptionOccurred{Connection: rt.connection, Job: rt, Err: cause})
	if o.sync {
		m.fail(ctx, rt, cause, o)
		return cause
	}
	if failed, ferr := rt.HasFailed(); failed {
		m.fail(ctx, rt, ferr, o)
		return cause
	}
	if errors.Is(cause, ErrJobTimeout) && p.FailOnTimeout {
		m.fail(ctx, rt, cause, o)
		return cause
	}
	if m.shouldNotRetry(cause) {
		m.fail(ctx, rt, cause, o)
		return cause
	}
	if p.MaxExceptions > 0 {
		n, err := m.cache.Increment(ctx, exceptionsKey(p), 1, 24*time.Hour)
		if err == nil && int(n) >= p.MaxExceptions {
			m.fail(ctx, rt, Wrap(MaxExceptionsExceeded, cause, "%s has thrown too many exceptions", p.DisplayName), o)
			return cause
		}
	}
	if m.willExceedAttempts(rt, maxTries) {
		m.fail(ctx, rt, cause, o)
		return cause
	}
	if released, delay := rt.IsReleased(); released {
		return errors.Join(cause, rt.reserved.Release(ctx, delay))
	}
	if rt.IsDeleted() {
		return errors.Join(cause, rt.reserved.Delete(ctx))
	}
	delay := p.backoffFor(rt.Attempts(), o.backoff)
	if err := rt.reserved.Release(ctx, delay); err != nil {
		return errors.Join(cause, err)
	}
	m.events.emit(ctx, JobReleasedAfterException{Connection: rt.connection, Job: rt, Delay: delay})
	return cause
}

func (m *Manager) willExceedAttempts(rt *Runtime, maxTries int) bool {
	if rt.payload.RetryUntil > 0 {
		return m.now().Unix() >= rt.payload.RetryUntil
	}
	return maxTries > 0 && rt.Attempts() >= maxTries
}

// fail marks a job failed for good: it is deleted, its Failed method and chain/batch failure
// callbacks run, it is stored in failed_jobs and JobFailed fires.
func (m *Manager) fail(ctx context.Context, rt *Runtime, cause error, o processOptions) {
	rt.mu.Lock()
	rt.failed, rt.failErr = true, cause
	rt.mu.Unlock()
	p := rt.payload
	if !o.sync {
		if err := rt.reserved.Delete(ctx); err != nil {
			m.log.Error("perago: delete failed job", "job", p.Job, "err", err)
		}
	}
	m.releaseUnique(ctx, rt)
	if p.MaxExceptions > 0 {
		_ = m.cache.Forget(ctx, exceptionsKey(p))
	}
	if f, ok := rt.job.(withFailed); ok {
		if err := safeCall(func() { f.Failed(ctx, cause) }); err != nil {
			m.log.Error("perago: failed() panicked", "job", p.Job, "err", err)
		}
	}
	if err := m.recordBatchFailure(ctx, p, cause); err != nil {
		m.log.Error("perago: record batch failure", "batch", p.BatchID, "err", err)
	}
	m.runCallbacks(ctx, p.ChainCatch, nil, cause)
	m.forgetLocal(p.ChainCatch)
	if o.logFailed {
		m.logFailed(ctx, FailedJob{ID: p.UUID, Connection: rt.connection, Queue: rt.Queue(), Payload: rt.reserved.Payload(), Exception: cause.Error(), FailedAt: m.now()})
	}
	m.events.emit(ctx, JobFailed{Connection: rt.connection, Job: rt, Err: cause})
}

func (m *Manager) logFailed(ctx context.Context, f FailedJob) {
	if err := m.failed.Log(ctx, f); err != nil {
		m.log.Error("perago: store failed job", "id", f.ID, "err", err)
	}
}

func (m *Manager) releaseUnique(ctx context.Context, rt *Runtime) {
	p := rt.payload
	if p.UniqueKey == "" {
		return
	}
	c := m.cache
	if rt.job != nil {
		c = m.uniqueCache(rt.job)
	}
	if err := c.Forget(ctx, p.UniqueKey); err != nil {
		m.log.Error("perago: release unique lock", "key", p.UniqueKey, "err", err)
	}
}

func (m *Manager) supersededByDebounce(ctx context.Context, rt *Runtime) (bool, error) {
	p := rt.payload
	if p.DebounceKey == "" {
		return false, nil
	}
	c := m.debounceCache(rt.job)
	latest, ok, err := c.Get(ctx, p.DebounceKey)
	if err != nil {
		return false, err
	}
	if ok && latest != p.UUID {
		m.events.emit(ctx, JobDebounced{Connection: rt.connection, Job: rt})
		return true, rt.reserved.Delete(ctx)
	}
	_ = c.Forget(ctx, p.DebounceKey)
	_ = c.Forget(ctx, p.DebounceKey+":first")
	return false, nil
}

// runJob sends the job through its middleware and Handle, bounded by timeout.
func (m *Manager) runJob(ctx context.Context, rt *Runtime, timeout time.Duration, o processOptions) error {
	job := rt.job
	var mws []Middleware
	if w, ok := job.(withMiddleware); ok {
		mws = w.Middleware()
	}
	var call func(i int) Next
	call = func(i int) Next {
		if i == len(mws) {
			return func(ctx context.Context) error { return safeHandle(ctx, job) }
		}
		return func(ctx context.Context) error {
			var err error
			if perr := safeCall(func() { err = mws[i].Handle(ctx, job, call(i+1)) }); perr != nil {
				return perr
			}
			return err
		}
	}
	run := call(0)
	if timeout <= 0 {
		return run(ctx)
	}
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(tctx) }()
	select {
	case err := <-done:
		return err
	case <-tctx.Done():
	}
	m.events.emit(ctx, JobTimedOut{Connection: rt.connection, Job: rt})
	grace := time.NewTimer(o.grace)
	defer grace.Stop()
	select {
	case err := <-done:
		m.log.Debug("perago: job returned after its timeout", "job", rt.payload.Job, "err", err)
	case <-grace.C:
		if o.leaked != nil {
			o.leaked.Store(true)
		}
	}
	return Newf(JobTimeout, "%s has timed out after %s", rt.payload.DisplayName, timeout)
}

func safeHandle(ctx context.Context, job Job) (err error) {
	if perr := safeCall(func() { err = job.Handle(ctx) }); perr != nil {
		return perr
	}
	return err
}

// safeCall turns a panic into an error carrying the stack.
func safeCall(fn func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	fn()
	return nil
}

// decodeJob rebuilds the job of a payload. consume drops an in-process closure once used.
func (m *Manager) decodeJob(_ context.Context, p *Payload, consume bool) (Job, error) {
	data := []byte(p.Data)
	if p.Secret != "" {
		plain, err := m.decrypt(p.Secret)
		if err != nil {
			return nil, err
		}
		data = plain
	}
	if p.Job == closureName {
		c := &closureJob{}
		if err := jsonUnmarshal(data, c); err != nil {
			return nil, err
		}
		c.fn = m.localFn(c.ID, consume)
		return c, nil
	}
	f, ok := lookupFactory(p.Job)
	if !ok {
		return nil, Newf(JobNotRegistered, "job %q is not registered in this process (perago.Register)", p.Job)
	}
	job := f()
	if len(data) > 0 {
		if err := jsonUnmarshal(data, job); err != nil {
			return nil, err
		}
	}
	return job, nil
}

func (m *Manager) decrypt(sealed string) ([]byte, error) {
	plain, err := open(m.cfg.Key, sealed)
	if err == nil || len(m.cfg.PreviousKeys) == 0 {
		return plain, err
	}
	for _, k := range m.cfg.PreviousKeys {
		if plain, perr := open(k, sealed); perr == nil {
			return plain, nil
		}
	}
	return nil, err
}

func (m *Manager) localFn(id string, consume bool) func(ctx context.Context) error {
	if consume {
		return m.takeLocal(id)
	}
	m.localMu.Lock()
	defer m.localMu.Unlock()
	return m.local[id]
}

// restoreModels calls Restore on every Restorable field of the job (embedded structs included).
func restoreModels(ctx context.Context, job Job) error {
	v := reflect.ValueOf(job)
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil
	}
	return restoreStruct(ctx, v)
}

var restorableType = reflect.TypeFor[Restorable]()

func restoreStruct(ctx context.Context, v reflect.Value) error {
	t := v.Type()
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() && !f.Anonymous {
			continue
		}
		fv := v.Field(i)
		if fv.CanAddr() && fv.Addr().Type().Implements(restorableType) && fv.Addr().CanInterface() {
			if err := fv.Addr().Interface().(Restorable).Restore(ctx); err != nil {
				return err
			}
			continue
		}
		if f.Anonymous && fv.Kind() == reflect.Struct {
			if err := restoreStruct(ctx, fv); err != nil {
				return err
			}
		}
	}
	return nil
}

var (
	memOnce  sync.Once
	memTotal uint64
)

func memoryExceeded(o *WorkerOptions) bool {
	if o.MemoryMB <= 0 && o.MemoryPercent <= 0 {
		return false
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	used := ms.Sys - ms.HeapReleased
	if o.MemoryMB > 0 && used >= uint64(o.MemoryMB)*1024*1024 {
		return true
	}
	if o.MemoryPercent <= 0 {
		return false
	}
	memOnce.Do(func() { memTotal = totalMemory() })
	return memTotal > 0 && float64(used) >= float64(memTotal)*o.MemoryPercent/100
}

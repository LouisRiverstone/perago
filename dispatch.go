package perago

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"
)

// PendingDispatch is a dispatch being configured; nothing is queued until Send.
type PendingDispatch struct {
	m   *Manager
	job Job

	connection   string
	queue        string
	delay        *time.Duration
	withoutDelay bool
	afterCommit  *bool
	group        string
	dedup        string
	condition    *bool

	chain      []any
	chainConn  string
	chainQueue string
	catch      []Job

	batchID       string
	sync          bool
	afterResponse bool
	uuid          string
}

// Dispatch starts dispatching job (ProcessPodcast::dispatch). Call Send to queue it.
func (m *Manager) Dispatch(job Job) *PendingDispatch { return &PendingDispatch{m: m, job: job} }

// DispatchIf dispatches job only when cond is true.
func (m *Manager) DispatchIf(ctx context.Context, cond bool, job Job) (string, error) {
	return m.Dispatch(job).If(cond).Send(ctx)
}

// DispatchUnless dispatches job only when cond is false.
func (m *Manager) DispatchUnless(ctx context.Context, cond bool, job Job) (string, error) {
	return m.Dispatch(job).If(!cond).Send(ctx)
}

// DispatchSync runs job now, in this goroutine, and returns its error (dispatchSync).
func (m *Manager) DispatchSync(ctx context.Context, job Job) error {
	pd := m.Dispatch(job)
	pd.sync = true
	_, err := pd.Send(ctx)
	return err
}

// DispatchAfterResponse runs job in this process once the current request is done
// (dispatchAfterResponse); see DeferredDriver.
func (m *Manager) DispatchAfterResponse(ctx context.Context, job Job) error {
	pd := m.Dispatch(job)
	pd.afterResponse = true
	_, err := pd.Send(ctx)
	return err
}

func (p *PendingDispatch) OnConnection(name string) *PendingDispatch { p.connection = name; return p }
func (p *PendingDispatch) OnQueue(name string) *PendingDispatch      { p.queue = name; return p }

// Delay makes the job available after d.
func (p *PendingDispatch) Delay(d time.Duration) *PendingDispatch { p.delay = &d; return p }

// DelayUntil makes the job available at t.
func (p *PendingDispatch) DelayUntil(t time.Time) *PendingDispatch {
	return p.Delay(time.Until(t))
}

// WithoutDelay ignores the job's default delay.
func (p *PendingDispatch) WithoutDelay() *PendingDispatch { p.withoutDelay = true; return p }

// AfterCommit holds the job until the surrounding Manager.Transaction commits.
func (p *PendingDispatch) AfterCommit() *PendingDispatch { t := true; p.afterCommit = &t; return p }

// BeforeCommit dispatches right away even on an after_commit connection.
func (p *PendingDispatch) BeforeCommit() *PendingDispatch { f := false; p.afterCommit = &f; return p }

// OnGroup sets the SQS message group (FIFO ordering, fair queues).
func (p *PendingDispatch) OnGroup(group string) *PendingDispatch { p.group = group; return p }

// WithDeduplicationID sets the SQS FIFO deduplication id.
func (p *PendingDispatch) WithDeduplicationID(id string) *PendingDispatch { p.dedup = id; return p }

// If skips the dispatch when cond is false.
func (p *PendingDispatch) If(cond bool) *PendingDispatch { p.condition = &cond; return p }

// Catch runs fn when the job fails for good (dispatch(fn)->catch(...)). fn is a Go closure, so it
// runs only when the failing worker is this process; use CatchJob otherwise.
func (p *PendingDispatch) Catch(fn func(ctx context.Context, err error)) *PendingDispatch {
	return p.CatchJob(Closure(func(ctx context.Context) error { fn(ctx, ErrorFrom(ctx)); return nil }))
}

// CatchJob runs job inline when the job fails for good; ErrorFrom(ctx) is the failure.
func (p *PendingDispatch) CatchJob(job Job) *PendingDispatch {
	p.catch = append(p.catch, job)
	return p
}

// Chain runs jobs (Job values or *PendingBatch) one after another once this job succeeds
// (withChain).
func (p *PendingDispatch) Chain(jobs ...any) *PendingDispatch {
	p.chain = append(p.chain, jobs...)
	return p
}

// Send queues the job and returns its UUID. An empty id with a nil error means the dispatch was
// skipped on purpose: a false condition, PrepareForDispatch returning false, or a unique job
// whose lock is held.
func (p *PendingDispatch) Send(ctx context.Context) (string, error) {
	if p.condition != nil && !*p.condition {
		return "", nil
	}
	return p.m.send(ctx, p)
}

func (m *Manager) send(ctx context.Context, pd *PendingDispatch) (string, error) {
	if pd.job == nil {
		return "", Newf(InvalidConfig, "dispatch of a nil job")
	}
	if pr, ok := pd.job.(withPrepare); ok && !pr.PrepareForDispatch(ctx) {
		return "", nil
	}
	if _, ok := pd.job.(ShouldNotQueue); ok {
		pd.sync = true
	}
	conn, queue, err := m.resolve(pd)
	if err != nil {
		return "", err
	}
	if handled, id := m.intercept(ctx, pd, conn, queue); handled {
		return id, nil
	}
	if pd.sync {
		return "", m.runNow(ctx, pd, conn, queue)
	}
	if pd.afterResponse {
		return m.deferRun(ctx, pd, conn, queue)
	}
	if tx := txFrom(ctx); tx != nil && m.waitsForCommit(pd, conn) {
		pd.uuid = newUUID()
		tx.add(func(ctx context.Context) error {
			_, err := m.push(ctx, pd, conn, queue)
			return err
		})
		return pd.uuid, nil
	}
	return m.push(ctx, pd, conn, queue)
}

func (m *Manager) waitsForCommit(pd *PendingDispatch, conn string) bool {
	if pd.afterCommit != nil {
		return *pd.afterCommit
	}
	if q, ok := pd.job.(queueableJob); ok && q.queueable().afterCommit != nil {
		return *q.queueable().afterCommit
	}
	if _, ok := pd.job.(ShouldQueueAfterCommit); ok {
		return true
	}
	return m.conns[conn].AfterCommit
}

// resolve picks the connection and queue: the dispatch, then the job's Queueable defaults,
// then Route, then the connection defaults, then Forward.
func (m *Manager) resolve(pd *PendingDispatch) (string, string, error) {
	conn, queue := pd.connection, pd.queue
	if q, ok := pd.job.(queueableJob); ok {
		if conn == "" {
			conn = q.queueable().connection
		}
		if queue == "" {
			queue = q.queueable().queue
		}
	}
	explicit := conn != ""
	if r, ok := m.routeFor(pd.job); ok {
		if conn == "" {
			conn = r.connection
		}
		if queue == "" {
			queue = r.queue
		}
	}
	if conn == "" {
		conn = m.def
	}
	c, ok := m.conns[conn]
	if !ok {
		return "", "", Newf(ConnectionNotFound, "queue connection %q is not configured", conn)
	}
	if queue == "" {
		queue = c.Queue
	}
	m.mu.RLock()
	f, ok := m.forwards[queue]
	m.mu.RUnlock()
	if !ok {
		return conn, queue, nil
	}
	queue = f.queue
	if f.connection != "" && !explicit {
		conn = f.connection
	}
	if _, ok := m.conns[conn]; !ok {
		return "", "", Newf(ConnectionNotFound, "queue connection %q is not configured", conn)
	}
	return conn, queue, nil
}

func (m *Manager) delayOf(pd *PendingDispatch) time.Duration {
	if pd.withoutDelay {
		return 0
	}
	if pd.delay != nil {
		return max(*pd.delay, 0)
	}
	if q, ok := pd.job.(queueableJob); ok && q.queueable().delay != nil {
		return max(*q.queueable().delay, 0)
	}
	return 0
}

func (m *Manager) push(ctx context.Context, pd *PendingDispatch, conn, queue string) (string, error) {
	p, err := m.buildPayload(pd, conn, queue, m.isLocal(conn))
	if err != nil {
		return "", err
	}
	delay := m.delayOf(pd)
	unlock := func() {}
	if p.UniqueKey != "" {
		lock := NewLock(m.uniqueCache(pd.job), p.UniqueKey, p.UUID, uniqueFor(pd.job))
		ok, err := lock.Get(ctx)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", nil
		}
		unlock = func() { _, _ = lock.Release(context.WithoutCancel(ctx)) }
	}
	if p.DebounceKey != "" {
		if delay, err = m.debounce(ctx, pd.job, p, delay); err != nil {
			unlock()
			return "", err
		}
	}
	m.events.emit(ctx, JobQueueing{Connection: conn, Queue: queue, Job: pd.job, Payload: p, Delay: delay})
	id, err := m.pushPayload(ctx, conn, p, delay)
	if err != nil {
		unlock()
		return "", err
	}
	m.events.emit(ctx, JobQueued{Connection: p.Connection, Queue: p.Queue, ID: id, Job: pd.job, Payload: p, Delay: delay})
	return p.UUID, nil
}

// pushPayload hands an encoded job to the connection's driver, running in-process
// connections itself.
func (m *Manager) pushPayload(ctx context.Context, conn string, p *Payload, delay time.Duration) (string, error) {
	c, ok := m.conns[conn]
	if !ok {
		return "", Newf(ConnectionNotFound, "queue connection %q is not configured", conn)
	}
	p.Connection = conn
	ld, isLocal := c.Driver.(localDriver)
	if isLocal && ld.kind == kindFailover {
		return m.pushFailover(ctx, ld, p, delay)
	}
	raw, err := p.encode()
	if err != nil {
		return "", err
	}
	if !isLocal {
		id, err := c.Driver.Push(ctx, Message{ID: p.UUID, Queue: p.Queue, Payload: raw, Delay: delay, Group: p.Group, DeduplicationID: p.DedupID})
		if err != nil {
			return "", Wrap(PushFailed, err, "push %s to %s:%s", p.Job, conn, p.Queue)
		}
		return id, nil
	}
	switch ld.kind {
	case kindSync:
		return p.UUID, m.runPayload(ctx, conn, p.Queue, raw, false)
	case kindDeferred:
		m.deferFn(ctx, func(ctx context.Context) { m.logLocal(m.runPayload(ctx, conn, p.Queue, raw, true)) })
		return p.UUID, nil
	}
	m.goBackground(ctx, func(ctx context.Context) {
		if delay > 0 {
			t := time.NewTimer(delay)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
		m.logLocal(m.runPayload(ctx, conn, p.Queue, raw, true))
	})
	return p.UUID, nil
}

func (m *Manager) pushFailover(ctx context.Context, d localDriver, p *Payload, delay time.Duration) (string, error) {
	var errs []error
	for _, name := range d.conns {
		id, err := m.pushPayload(ctx, name, p, delay)
		if err == nil {
			return id, nil
		}
		errs = append(errs, err)
		m.events.emit(ctx, QueueFailedOver{Connection: name, Err: err})
	}
	return "", Wrap(FailoverExhausted, errors.Join(errs...), "every failover connection rejected %s", p.Job)
}

func (m *Manager) logLocal(err error) {
	if err != nil {
		m.log.Error("perago: in-process job failed", "err", err)
	}
}

// runPayload processes an encoded job once, in this goroutine (sync semantics).
func (m *Manager) runPayload(ctx context.Context, conn, queue string, raw []byte, logFailed bool) error {
	p, err := DecodePayload(raw)
	if err != nil {
		return err
	}
	r := &memoryReserved{id: p.UUID, queue: queue, payload: raw, attempts: 1}
	return m.process(ctx, conn, r, processOptions{sync: true, maxTries: 1, logFailed: logFailed})
}

func (m *Manager) runNow(ctx context.Context, pd *PendingDispatch, conn, queue string) error {
	p, err := m.buildPayload(pd, conn, queue, true)
	if err != nil {
		return err
	}
	p.UniqueKey, p.DebounceKey = "", ""
	raw, err := p.encode()
	if err != nil {
		return err
	}
	return m.runPayload(ctx, conn, queue, raw, false)
}

func (m *Manager) deferRun(ctx context.Context, pd *PendingDispatch, conn, queue string) (string, error) {
	p, err := m.buildPayload(pd, conn, queue, true)
	if err != nil {
		return "", err
	}
	p.UniqueKey, p.DebounceKey = "", ""
	raw, err := p.encode()
	if err != nil {
		return "", err
	}
	m.deferFn(ctx, func(ctx context.Context) { m.logLocal(m.runPayload(ctx, conn, queue, raw, true)) })
	return p.UUID, nil
}

func (m *Manager) goBackground(ctx context.Context, fn func(ctx context.Context)) {
	ctx = context.WithoutCancel(ctx)
	m.bg.Add(1)
	go func() {
		defer m.bg.Done()
		fn(ctx)
	}()
}

// buildPayload encodes a job and resolves its settings. local allows closures.
func (m *Manager) buildPayload(pd *PendingDispatch, conn, queue string, local bool) (*Payload, error) {
	job := pd.job
	id := pd.uuid
	if id == "" {
		id = newUUID()
	}
	p := &Payload{
		UUID: id, Job: NameOf(job), DisplayName: DisplayNameOf(job),
		Connection: conn, Queue: queue, CreatedAt: m.now().Unix(), BatchID: pd.batchID,
	}
	data, err := m.encodeJob(job, local)
	if err != nil {
		return nil, err
	}
	p.Data = data
	if _, ok := job.(ShouldBeEncrypted); ok {
		sealed, err := seal(m.cfg.Key, data)
		if err != nil {
			return nil, err
		}
		p.Data, p.Secret = nil, sealed
	}
	m.applySettings(job, p)
	p.Group, p.DedupID = pd.group, pd.dedup
	if q, ok := job.(queueableJob); ok && p.Group == "" {
		p.Group = q.queueable().group
	}
	if g, ok := job.(withMessageGroup); ok && p.Group == "" {
		p.Group = g.MessageGroup()
	}
	if d, ok := job.(withDeduplicationID); ok && p.DedupID == "" {
		p.DedupID = d.DeduplicationID()
	}
	if _, ok := job.(ShouldBeUnique); ok && pd.batchID == "" {
		p.UniqueKey = "perago:unique:" + p.Job + ":" + uniqueID(job)
		_, p.UniqueUntilProcessing = job.(ShouldBeUniqueUntilProcessing)
	}
	if _, ok := job.(withDebounceFor); ok && pd.batchID == "" {
		p.DebounceKey = "perago:debounce:" + p.Job + ":" + debounceID(job)
	}
	for _, cb := range pd.catch {
		raw, err := m.encodeCallback(cb, conn, queue)
		if err != nil {
			return nil, err
		}
		p.ChainCatch = append(p.ChainCatch, raw)
	}
	if len(pd.chain) == 0 {
		return p, nil
	}
	p.ChainConnection, p.ChainQueue = pd.chainConn, pd.chainQueue
	if p.Chain, err = m.encodeChain(pd.chain, pd.chainConn, pd.chainQueue, pd.batchID); err != nil {
		return nil, err
	}
	return p, nil
}

func (m *Manager) encodeJob(job Job, local bool) (json.RawMessage, error) {
	if c, ok := job.(*closureJob); ok {
		if !local {
			return nil, Newf(ClosureNotSerializable,
				"a perago.Closure only runs on sync, deferred or background connections; use RegisterClosure and Call")
		}
		if c.ID == "" {
			c.ID = m.storeLocal(c.fn)
		}
		return json.Marshal(c)
	}
	name := NameOf(job)
	if _, ok := lookupFactory(name); !ok {
		RegisterAs(name, job)
	}
	data, err := json.Marshal(job)
	if err != nil {
		return nil, Wrap(PayloadInvalid, err, "encode %s", name)
	}
	return data, nil
}

func (m *Manager) applySettings(job Job, p *Payload) {
	if t, ok := job.(withTries); ok {
		n := t.Tries()
		p.MaxTries = &n
	}
	if b, ok := job.(withBackoff); ok {
		for _, d := range b.Backoff() {
			p.Backoff = append(p.Backoff, d.Milliseconds())
		}
	}
	if t, ok := job.(withTimeout); ok {
		p.Timeout = t.Timeout().Milliseconds()
	}
	if r, ok := job.(withRetryFor); ok {
		p.RetryUntil = m.now().Add(r.RetryFor()).Unix()
	}
	if r, ok := job.(withRetryUntil); ok {
		if t := r.RetryUntil(); !t.IsZero() {
			p.RetryUntil = t.Unix()
		}
	}
	if e, ok := job.(withMaxExceptions); ok {
		p.MaxExceptions = e.MaxExceptions()
	}
	if f, ok := job.(withFailOnTimeout); ok {
		p.FailOnTimeout = f.FailOnTimeout()
	}
	if c, ok := job.(withCountCrashes); ok {
		p.CountCrashes = c.CountCrashesAsExceptions()
	}
	if d, ok := job.(withDeleteMissing); ok {
		p.DeleteMissing = d.DeleteWhenMissingModels()
	}
}

func (m *Manager) encodeChain(jobs []any, chainConn, chainQueue, batchID string) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, 0, len(jobs))
	for _, item := range jobs {
		var job Job
		switch v := item.(type) {
		case *PendingBatch:
			cb, err := v.toChained()
			if err != nil {
				return nil, err
			}
			job = cb
		case Job:
			job = v
		default:
			return nil, Newf(InvalidConfig, "chain items must be jobs or batches, got %T", item)
		}
		next := &PendingDispatch{m: m, job: job, connection: chainConn, queue: chainQueue, batchID: batchID}
		conn, queue, err := m.resolve(next)
		if err != nil {
			return nil, err
		}
		p, err := m.buildPayload(next, conn, queue, m.isLocal(conn))
		if err != nil {
			return nil, err
		}
		p.Delay = m.delayOf(next).Milliseconds()
		raw, err := p.encode()
		if err != nil {
			return nil, err
		}
		out = append(out, raw)
	}
	return out, nil
}

// encodeCallback encodes a chain or batch callback. Closures are allowed on any connection: they
// run when the worker that finishes the chain or batch lives in this process (an app running its
// workers in-process), and are reported as missing otherwise.
func (m *Manager) encodeCallback(cb Job, conn, queue string) (json.RawMessage, error) {
	p, err := m.buildPayload(&PendingDispatch{m: m, job: cb}, conn, queue, true)
	if err != nil {
		return nil, err
	}
	return p.encode()
}

func uniqueID(job Job) string {
	if u, ok := job.(withUniqueID); ok {
		return u.UniqueID()
	}
	return ""
}

func uniqueFor(job Job) time.Duration {
	if u, ok := job.(withUniqueFor); ok {
		return u.UniqueFor()
	}
	return 0
}

func (m *Manager) uniqueCache(job Job) Cache {
	if u, ok := job.(withUniqueVia); ok && u.UniqueVia() != nil {
		return u.UniqueVia()
	}
	return m.cache
}

func debounceID(job Job) string {
	if d, ok := job.(withDebounceID); ok {
		return d.DebounceID()
	}
	return ""
}

func (m *Manager) debounceCache(job Job) Cache {
	if d, ok := job.(withDebounceVia); ok && d.DebounceVia() != nil {
		return d.DebounceVia()
	}
	return m.cache
}

// debounce records p as the latest dispatch of its key and returns the delay to use:
// DebounceFor, shortened so the job runs no later than DebounceMaxWait after the first dispatch.
func (m *Manager) debounce(ctx context.Context, job Job, p *Payload, delay time.Duration) (time.Duration, error) {
	window := job.(withDebounceFor).DebounceFor()
	c := m.debounceCache(job)
	ttl := window + 24*time.Hour
	var maxWait time.Duration
	if w, ok := job.(withDebounceMaxWait); ok {
		maxWait = w.DebounceMaxWait()
	}
	if maxWait > 0 {
		ttl = maxWait + window + 24*time.Hour
		now := m.now()
		if _, err := c.Add(ctx, p.DebounceKey+":first", formatInt(now.UnixNano()), ttl); err != nil {
			return 0, err
		}
		first, _, err := c.Get(ctx, p.DebounceKey+":first")
		if err != nil {
			return 0, err
		}
		deadline := time.Unix(0, parseInt(first)).Add(maxWait)
		window = min(window, max(deadline.Sub(now), 0))
	}
	if err := c.Put(ctx, p.DebounceKey, p.UUID, ttl); err != nil {
		return 0, err
	}
	return max(delay, window), nil
}

// Bulk pushes many independent jobs, grouped by connection and queue (Bus::bulk).
// It has none of a batch's tracking or callbacks.
func (m *Manager) Bulk(ctx context.Context, jobs ...Job) error {
	type group struct {
		conn, queue string
		msgs        []Message
	}
	groups := map[[2]string]*group{}
	var order [][2]string
	for _, job := range jobs {
		pd := m.Dispatch(job)
		conn, queue, err := m.resolve(pd)
		if err != nil {
			return err
		}
		if handled, _ := m.intercept(ctx, pd, conn, queue); handled {
			continue
		}
		c := m.conns[conn]
		if _, ok := c.Driver.(localDriver); ok {
			if _, err := m.push(ctx, pd, conn, queue); err != nil {
				return err
			}
			continue
		}
		p, err := m.buildPayload(pd, conn, queue, false)
		if err != nil {
			return err
		}
		raw, err := p.encode()
		if err != nil {
			return err
		}
		k := [2]string{conn, queue}
		g, ok := groups[k]
		if !ok {
			g = &group{conn: conn, queue: queue}
			groups[k] = g
			order = append(order, k)
		}
		g.msgs = append(g.msgs, Message{ID: p.UUID, Queue: queue, Payload: raw, Delay: m.delayOf(pd), Group: p.Group, DeduplicationID: p.DedupID})
	}
	for _, k := range order {
		g := groups[k]
		d := m.conns[g.conn].Driver
		if bp, ok := d.(BulkPusher); ok {
			if _, err := bp.PushBulk(ctx, g.msgs); err != nil {
				return Wrap(PushFailed, err, "bulk push to %s:%s", g.conn, g.queue)
			}
			continue
		}
		for _, msg := range g.msgs {
			if _, err := d.Push(ctx, msg); err != nil {
				return Wrap(PushFailed, err, "bulk push to %s:%s", g.conn, g.queue)
			}
		}
	}
	return nil
}

type deferredKey struct{}

type deferredScope struct {
	mu  sync.Mutex
	fns []func(ctx context.Context)
}

// WithDeferred opens a scope for deferred jobs: they run when the returned flush is called.
func WithDeferred(ctx context.Context) (context.Context, func()) {
	s := &deferredScope{}
	return context.WithValue(ctx, deferredKey{}, s), func() {
		for {
			s.mu.Lock()
			fns := s.fns
			s.fns = nil
			s.mu.Unlock()
			if len(fns) == 0 {
				return
			}
			for _, fn := range fns {
				fn(context.WithoutCancel(ctx))
			}
		}
	}
}

func (m *Manager) deferFn(ctx context.Context, fn func(ctx context.Context)) {
	s, ok := ctx.Value(deferredKey{}).(*deferredScope)
	if !ok {
		m.goBackground(ctx, fn)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fns = append(s.fns, fn)
}

// Middleware runs the deferred jobs of each request after its handler returns.
func (m *Manager) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, flush := WithDeferred(r.Context())
		next.ServeHTTP(w, r.WithContext(ctx))
		m.goBackground(ctx, func(context.Context) { flush() })
	})
}

type txKey struct{}

// Tx buffers the dispatches of an after-commit scope.
type Tx struct {
	mu     sync.Mutex
	parent *Tx
	fns    []func(ctx context.Context) error
	done   bool
}

func txFrom(ctx context.Context) *Tx {
	t, _ := ctx.Value(txKey{}).(*Tx)
	return t
}

func (t *Tx) add(fn func(ctx context.Context) error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.fns = append(t.fns, fn)
}

// BeginTransaction opens an after-commit scope. Dispatches made with the returned context to an
// AfterCommit connection (or with AfterCommit()) wait for Commit; Rollback drops them.
// Scopes nest: an inner Commit hands its jobs to the outer scope.
func BeginTransaction(ctx context.Context) (context.Context, *Tx) {
	t := &Tx{parent: txFrom(ctx)}
	return context.WithValue(ctx, txKey{}, t), t
}

// Commit dispatches the buffered jobs, or passes them to the enclosing scope.
func (t *Tx) Commit(ctx context.Context) error {
	t.mu.Lock()
	fns := t.fns
	t.fns, t.done = nil, true
	t.mu.Unlock()
	if t.parent != nil {
		for _, fn := range fns {
			t.parent.add(fn)
		}
		return nil
	}
	var errs []error
	for _, fn := range fns {
		if err := fn(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Rollback drops the buffered jobs.
func (t *Tx) Rollback() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.fns, t.done = nil, true
}

// Transaction runs fn in an after-commit scope: when fn returns nil the buffered jobs are
// dispatched, otherwise they are dropped. Run your database transaction inside fn with the
// context it receives (database.Transaction does both for a migro database).
func (m *Manager) Transaction(ctx context.Context, fn func(ctx context.Context) error) error {
	ctx, tx := BeginTransaction(ctx)
	if err := fn(ctx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit(context.WithoutCancel(ctx))
}

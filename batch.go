package perago

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// BatchOptions is what a batch stores besides its counters: where its jobs go and its callbacks
// (encoded callback jobs).
type BatchOptions struct {
	Connection    string            `json:"connection,omitempty"`
	Queue         string            `json:"queue,omitempty"`
	AllowFailures bool              `json:"allowFailures,omitempty"`
	Then          []json.RawMessage `json:"then,omitempty"`
	Catch         []json.RawMessage `json:"catch,omitempty"`
	Finally       []json.RawMessage `json:"finally,omitempty"`
	Progress      []json.RawMessage `json:"progress,omitempty"`

	// Chain is the rest of a chain that contains this batch: it runs once the batch finishes
	// without being cancelled.
	Chain           []json.RawMessage `json:"chain,omitempty"`
	ChainConnection string            `json:"chainConnection,omitempty"`
	ChainQueue      string            `json:"chainQueue,omitempty"`
	ChainCatch      []json.RawMessage `json:"chainCatch,omitempty"`
}

// Batch is a stored job batch (Illuminate\Bus\Batch).
type Batch struct {
	ID           string
	Name         string
	TotalJobs    int
	PendingJobs  int
	FailedJobs   int
	FailedJobIDs []string
	Options      BatchOptions
	CreatedAt    time.Time
	CancelledAt  *time.Time
	FinishedAt   *time.Time

	m        *Manager
	recorder *BatchRecorder
}

// MarshalJSON renders the batch like Laravel's Batch::toArray, so it can be returned from an
// HTTP handler as is.
func (b *Batch) MarshalJSON() ([]byte, error) {
	unix := func(t *time.Time) *int64 {
		if t == nil {
			return nil
		}
		v := t.Unix()
		return &v
	}
	return json.Marshal(struct {
		ID            string       `json:"id"`
		Name          string       `json:"name"`
		TotalJobs     int          `json:"totalJobs"`
		PendingJobs   int          `json:"pendingJobs"`
		ProcessedJobs int          `json:"processedJobs"`
		Progress      int          `json:"progress"`
		FailedJobs    int          `json:"failedJobs"`
		Options       batchSummary `json:"options"`
		CreatedAt     int64        `json:"createdAt"`
		CancelledAt   *int64       `json:"cancelledAt"`
		FinishedAt    *int64       `json:"finishedAt"`
	}{b.ID, b.Name, b.TotalJobs, b.PendingJobs, b.ProcessedJobs(), b.Progress(), b.FailedJobs,
		batchSummary{b.Options.Connection, b.Options.Queue, b.Options.AllowFailures},
		b.CreatedAt.Unix(), unix(b.CancelledAt), unix(b.FinishedAt)})
}

type batchSummary struct {
	Connection    string `json:"connection,omitempty"`
	Queue         string `json:"queue,omitempty"`
	AllowFailures bool   `json:"allowFailures"`
}

// ProcessedJobs is how many jobs have run (successfully or not).
func (b *Batch) ProcessedJobs() int { return b.TotalJobs - b.PendingJobs }

// Progress is the completion percentage, 0 to 100.
func (b *Batch) Progress() int {
	if b.TotalJobs == 0 {
		return 0
	}
	return b.ProcessedJobs() * 100 / b.TotalJobs
}

// HasPendingJobs reports whether jobs are still waiting.
func (b *Batch) HasPendingJobs() bool { return b.PendingJobs > 0 }

// Finished reports whether every job has run successfully.
func (b *Batch) Finished() bool { return b.FinishedAt != nil }

// HasFailures reports whether a job failed.
func (b *Batch) HasFailures() bool { return b.FailedJobs > 0 }

// Cancelled reports whether the batch was cancelled.
func (b *Batch) Cancelled() bool {
	return b.CancelledAt != nil || (b.recorder != nil && b.recorder.Cancelled)
}

func (b *Batch) bind(m *Manager) *Batch { b.m = m; return b }

// Fresh reloads the batch from the store.
func (b *Batch) Fresh(ctx context.Context) (*Batch, error) {
	if b.recorder != nil {
		return b, nil
	}
	return b.m.FindBatch(ctx, b.ID)
}

// Cancel cancels the batch; its jobs should check Cancelled (or use
// middleware.SkipIfBatchCancelled).
func (b *Batch) Cancel(ctx context.Context) error {
	if b.recorder != nil {
		b.recorder.Cancelled = true
		return nil
	}
	now := b.m.now()
	if err := b.m.batches.Cancel(ctx, b.ID, now); err != nil {
		return err
	}
	b.CancelledAt = &now
	return nil
}

// Delete removes the batch record.
func (b *Batch) Delete(ctx context.Context) error {
	if b.recorder != nil {
		b.recorder.Deleted = true
		return nil
	}
	return b.m.batches.Delete(ctx, b.ID)
}

// Add queues more jobs on the batch, typically from one of its jobs. Items are Job values or
// []Job chains.
func (b *Batch) Add(ctx context.Context, items ...any) error {
	if b.recorder != nil {
		b.recorder.Added = append(b.recorder.Added, items...)
		return nil
	}
	raws, count, err := b.m.encodeBatchItems(items, b.Options.Connection, b.Options.Queue)
	if err != nil {
		return err
	}
	return b.m.addEncoded(ctx, b, raws, count)
}

// BatchRecorder records what a job does to a fake batch (queuetest.WithFakeBatch).
type BatchRecorder struct {
	Added     []any
	Cancelled bool
	Deleted   bool
}

// NewFakeBatch returns a batch that only records Add, Cancel and Delete.
func NewFakeBatch(id, name string) (*Batch, *BatchRecorder) {
	r := &BatchRecorder{}
	return &Batch{ID: id, Name: name, CreatedAt: time.Now(), recorder: r}, r
}

// BatchStore persists batches (Laravel's BatchRepository).
type BatchStore interface {
	Store(ctx context.Context, b *Batch) error
	// Find returns ErrBatchNotFound when the batch does not exist.
	Find(ctx context.Context, id string) (*Batch, error)
	// List returns batches newest first, up to limit, older than the batch id before ("" = newest).
	List(ctx context.Context, limit int, before string) ([]*Batch, error)
	IncrementTotalJobs(ctx context.Context, id string, n int) error
	// RecordSuccess decrements the pending jobs, drops jobID from the failed ids and returns the
	// updated batch.
	RecordSuccess(ctx context.Context, id, jobID string) (*Batch, error)
	// RecordFailure increments the failed jobs, records jobID and returns the updated batch.
	RecordFailure(ctx context.Context, id, jobID string) (*Batch, error)
	MarkFinished(ctx context.Context, id string, at time.Time) error
	Cancel(ctx context.Context, id string, at time.Time) error
	Delete(ctx context.Context, id string) error
	// Prune deletes finished batches finished before t.
	Prune(ctx context.Context, before time.Time) (int64, error)
	// PruneUnfinished deletes unfinished batches created before t.
	PruneUnfinished(ctx context.Context, before time.Time) (int64, error)
	// PruneCancelled deletes cancelled batches cancelled before t.
	PruneCancelled(ctx context.Context, before time.Time) (int64, error)
}

// PendingBatch is a batch being configured (Bus::batch).
type PendingBatch struct {
	m             *Manager
	jobs          []any
	name          string
	conn          string
	queue         string
	allowFailures bool
	before        []func(ctx context.Context, b *Batch)
	then          []Job
	catch         []Job
	finally       []Job
	progress      []Job
}

// Batch groups jobs whose completion is tracked together (Bus::batch). Items are Job values or
// []Job chains.
func (m *Manager) Batch(items ...any) *PendingBatch { return &PendingBatch{m: m, jobs: items} }

func (p *PendingBatch) Name(name string) *PendingBatch         { p.name = name; return p }
func (p *PendingBatch) OnConnection(name string) *PendingBatch { p.conn = name; return p }
func (p *PendingBatch) OnQueue(name string) *PendingBatch      { p.queue = name; return p }
func (p *PendingBatch) AllowFailures(allow ...bool) *PendingBatch {
	p.allowFailures = firstOr(allow, true)
	return p
}

// Before runs fn in this process once the batch is stored, before its jobs are queued.
func (p *PendingBatch) Before(fn func(ctx context.Context, b *Batch)) *PendingBatch {
	p.before = append(p.before, fn)
	return p
}

// Then runs fn when every job succeeded. Closures only run in this process: see ThenJob.
func (p *PendingBatch) Then(fn func(ctx context.Context, b *Batch)) *PendingBatch {
	return p.ThenJob(batchClosure(func(ctx context.Context) { fn(ctx, BatchFrom(ctx)) }))
}

// Catch runs fn on the first job failure.
func (p *PendingBatch) Catch(fn func(ctx context.Context, b *Batch, err error)) *PendingBatch {
	return p.CatchJob(batchClosure(func(ctx context.Context) { fn(ctx, BatchFrom(ctx), ErrorFrom(ctx)) }))
}

// Finally runs fn once every job has run, whatever the outcome.
func (p *PendingBatch) Finally(fn func(ctx context.Context, b *Batch)) *PendingBatch {
	return p.FinallyJob(batchClosure(func(ctx context.Context) { fn(ctx, BatchFrom(ctx)) }))
}

// Progress runs fn after each job completes.
func (p *PendingBatch) Progress(fn func(ctx context.Context, b *Batch)) *PendingBatch {
	return p.ProgressJob(batchClosure(func(ctx context.Context) { fn(ctx, BatchFrom(ctx)) }))
}

// ThenJob, CatchJob, FinallyJob and ProgressJob register serializable callbacks: the job runs
// inline in the worker that completes the batch, with BatchFrom(ctx) and ErrorFrom(ctx).
func (p *PendingBatch) ThenJob(job Job) *PendingBatch  { p.then = append(p.then, job); return p }
func (p *PendingBatch) CatchJob(job Job) *PendingBatch { p.catch = append(p.catch, job); return p }
func (p *PendingBatch) FinallyJob(job Job) *PendingBatch {
	p.finally = append(p.finally, job)
	return p
}
func (p *PendingBatch) ProgressJob(job Job) *PendingBatch {
	p.progress = append(p.progress, job)
	return p
}

func batchClosure(fn func(ctx context.Context)) Job {
	return Closure(func(ctx context.Context) error { fn(ctx); return nil })
}

// Jobs is the batch's items.
func (p *PendingBatch) Jobs() []any { return p.jobs }

// BatchName is the batch name.
func (p *PendingBatch) BatchName() string { return p.name }

// AllowsFailures reports whether failures leave the batch running.
func (p *PendingBatch) AllowsFailures() bool { return p.allowFailures }

// Target is the connection and queue the batch dispatches to.
func (p *PendingBatch) Target() (connection, queue string) { return p.conn, p.queue }

// DispatchIf dispatches the batch only when cond is true.
func (p *PendingBatch) DispatchIf(ctx context.Context, cond bool) (*Batch, error) {
	if !cond {
		return nil, nil
	}
	return p.Dispatch(ctx)
}

// Dispatch stores the batch and queues its jobs.
func (p *PendingBatch) Dispatch(ctx context.Context) (*Batch, error) {
	if b, ok := p.m.interceptBatch(ctx, p); ok {
		return b, nil
	}
	conn, queue, err := p.target()
	if err != nil {
		return nil, err
	}
	opts, err := p.options(conn, queue)
	if err != nil {
		return nil, err
	}
	raws, count, err := p.m.encodeBatchItems(p.jobs, p.conn, p.queue)
	if err != nil {
		return nil, err
	}
	return p.m.dispatchBatch(ctx, p.name, opts, raws, count, p.before)
}

func (p *PendingBatch) target() (string, string, error) {
	conn := p.conn
	if conn == "" {
		conn = p.m.def
	}
	c, ok := p.m.conns[conn]
	if !ok {
		return "", "", Newf(ConnectionNotFound, "queue connection %q is not configured", conn)
	}
	queue := p.queue
	if queue == "" {
		queue = c.Queue
	}
	return conn, queue, nil
}

func (p *PendingBatch) options(conn, queue string) (BatchOptions, error) {
	opts := BatchOptions{Connection: p.conn, Queue: p.queue, AllowFailures: p.allowFailures}
	lists := []struct {
		in  []Job
		out *[]json.RawMessage
	}{{p.then, &opts.Then}, {p.catch, &opts.Catch}, {p.finally, &opts.Finally}, {p.progress, &opts.Progress}}
	for _, l := range lists {
		for _, cb := range l.in {
			raw, err := p.m.encodeCallback(cb, conn, queue)
			if err != nil {
				return opts, err
			}
			*l.out = append(*l.out, raw)
		}
	}
	return opts, nil
}

func (m *Manager) dispatchBatch(ctx context.Context, name string, opts BatchOptions, raws []json.RawMessage, count int, before []func(context.Context, *Batch)) (*Batch, error) {
	if m.batches == nil {
		return nil, Newf(InvalidConfig, "job batching needs Config.Batches (database.NewBatchStore, dynamodb.NewBatchStore or perago.NewMemoryBatchStore)")
	}
	b := &Batch{ID: newUUID(), Name: name, Options: opts, CreatedAt: m.now(), m: m}
	if err := m.batches.Store(ctx, b); err != nil {
		return nil, err
	}
	for _, fn := range before {
		fn(ctx, b)
	}
	if err := m.addEncoded(ctx, b, raws, count); err != nil {
		_ = m.batches.Delete(context.WithoutCancel(ctx), b.ID)
		return nil, err
	}
	m.events.emit(ctx, BatchDispatched{Batch: b})
	if count == 0 {
		return b, m.finishEmptyBatch(ctx, b)
	}
	return b, nil
}

func (m *Manager) finishEmptyBatch(ctx context.Context, b *Batch) error {
	now := m.now()
	if err := m.batches.MarkFinished(ctx, b.ID, now); err != nil {
		return err
	}
	b.FinishedAt = &now
	m.runCallbacks(ctx, b.Options.Then, b, nil)
	m.runCallbacks(ctx, b.Options.Finally, b, nil)
	return m.continueChainAfterBatch(ctx, b)
}

// encodeBatchItems encodes jobs and []Job chains; count is every job, chain members included.
func (m *Manager) encodeBatchItems(items []any, conn, queue string) ([]json.RawMessage, int, error) {
	var out []json.RawMessage
	count := 0
	for _, item := range items {
		jobs, err := batchItemJobs(item)
		if err != nil {
			return nil, 0, err
		}
		if len(jobs) == 0 {
			continue
		}
		head := &PendingDispatch{m: m, job: jobs[0], connection: conn, queue: queue, batchID: "pending"}
		head.chain, head.chainConn, head.chainQueue = toAny(jobs[1:]), conn, queue
		c, q, err := m.resolve(head)
		if err != nil {
			return nil, 0, err
		}
		p, err := m.buildPayload(head, c, q, m.isLocal(c))
		if err != nil {
			return nil, 0, err
		}
		p.Delay = m.delayOf(head).Milliseconds()
		raw, err := p.encode()
		if err != nil {
			return nil, 0, err
		}
		out = append(out, raw)
		count += len(jobs)
	}
	return out, count, nil
}

func batchItemJobs(item any) ([]Job, error) {
	switch v := item.(type) {
	case Job:
		return []Job{v}, nil
	case []Job:
		return v, nil
	case []any:
		jobs := make([]Job, 0, len(v))
		for _, x := range v {
			j, ok := x.(Job)
			if !ok {
				return nil, Newf(InvalidConfig, "batched chains hold jobs, got %T", x)
			}
			jobs = append(jobs, j)
		}
		return jobs, nil
	}
	return nil, Newf(InvalidConfig, "batch items must be jobs or []Job chains, got %T", item)
}

func toAny(jobs []Job) []any {
	out := make([]any, len(jobs))
	for i, j := range jobs {
		out[i] = j
	}
	return out
}

func (m *Manager) addEncoded(ctx context.Context, b *Batch, raws []json.RawMessage, count int) error {
	if count == 0 {
		return nil
	}
	if err := m.batches.IncrementTotalJobs(ctx, b.ID, count); err != nil {
		return err
	}
	b.TotalJobs += count
	b.PendingJobs += count
	for _, raw := range raws {
		p, err := DecodePayload(raw)
		if err != nil {
			return err
		}
		if err := setBatchID(p, b.ID); err != nil {
			return err
		}
		if _, err := m.pushPayload(ctx, p.Connection, p, time.Duration(p.Delay)*time.Millisecond); err != nil {
			return err
		}
	}
	return nil
}

func setBatchID(p *Payload, id string) error {
	p.BatchID = id
	for i, raw := range p.Chain {
		c, err := DecodePayload(raw)
		if err != nil {
			return err
		}
		c.BatchID = id
		if p.Chain[i], err = c.encode(); err != nil {
			return err
		}
	}
	return nil
}

// FindBatch loads a batch (Bus::findBatch).
func (m *Manager) FindBatch(ctx context.Context, id string) (*Batch, error) {
	if m.batches == nil {
		return nil, Newf(InvalidConfig, "job batching needs Config.Batches")
	}
	b, err := m.batches.Find(ctx, id)
	if err != nil {
		return nil, err
	}
	return b.bind(m), nil
}

// Batches lists stored batches newest first (queue:batches).
func (m *Manager) Batches(ctx context.Context, limit int, before string) ([]*Batch, error) {
	if m.batches == nil {
		return nil, Newf(InvalidConfig, "job batching needs Config.Batches")
	}
	bs, err := m.batches.List(ctx, limit, before)
	if err != nil {
		return nil, err
	}
	for _, b := range bs {
		b.bind(m)
	}
	return bs, nil
}

func (m *Manager) recordBatchSuccess(ctx context.Context, p *Payload) error {
	if p.BatchID == "" || m.batches == nil {
		return nil
	}
	b, err := m.batches.RecordSuccess(ctx, p.BatchID, p.UUID)
	if err != nil {
		return err
	}
	b.bind(m)
	m.runCallbacks(ctx, b.Options.Progress, b, nil)
	if b.PendingJobs == 0 {
		now := m.now()
		if err := m.batches.MarkFinished(ctx, b.ID, now); err != nil {
			return err
		}
		b.FinishedAt = &now
		m.runCallbacks(ctx, b.Options.Then, b, nil)
	}
	if b.PendingJobs-b.FailedJobs == 0 {
		m.runCallbacks(ctx, b.Options.Finally, b, nil)
		return m.continueChainAfterBatch(ctx, b)
	}
	return nil
}

func (m *Manager) recordBatchFailure(ctx context.Context, p *Payload, cause error) error {
	if p.BatchID == "" || m.batches == nil {
		return nil
	}
	b, err := m.batches.RecordFailure(ctx, p.BatchID, p.UUID)
	if err != nil {
		return err
	}
	b.bind(m)
	if b.Options.AllowFailures {
		m.runCallbacks(ctx, b.Options.Progress, b, cause)
	}
	if b.FailedJobs == 1 && !b.Options.AllowFailures {
		if err := b.Cancel(ctx); err != nil {
			return err
		}
	}
	if b.FailedJobs == 1 {
		m.runCallbacks(ctx, b.Options.Catch, b, cause)
	}
	if b.PendingJobs-b.FailedJobs == 0 {
		m.runCallbacks(ctx, b.Options.Finally, b, cause)
		return m.continueChainAfterBatch(ctx, b)
	}
	return nil
}

// forgetBatchClosures drops the batch's in-process callbacks once none can run again: the batch
// finished, or it was cancelled and every job has run.
func (m *Manager) forgetBatchClosures(b *Batch) {
	for _, raws := range [][]json.RawMessage{b.Options.Then, b.Options.Catch, b.Options.Finally, b.Options.Progress} {
		m.forgetLocal(raws)
	}
}

func (m *Manager) continueChainAfterBatch(ctx context.Context, b *Batch) error {
	if b.Finished() || b.Cancelled() {
		m.forgetBatchClosures(b)
	}
	if len(b.Options.Chain) == 0 || b.Cancelled() {
		return nil
	}
	head := &Payload{
		Chain: b.Options.Chain, ChainConnection: b.Options.ChainConnection,
		ChainQueue: b.Options.ChainQueue, ChainCatch: b.Options.ChainCatch,
	}
	return m.dispatchNextInChain(ctx, head)
}

// runCallbacks runs encoded callback jobs inline; their errors are logged.
func (m *Manager) runCallbacks(ctx context.Context, raws []json.RawMessage, b *Batch, cause error) {
	ctx = context.WithValue(ctx, batchKey{}, b)
	if cause != nil {
		ctx = context.WithValue(ctx, errorKey{}, cause)
	}
	for _, raw := range raws {
		p, err := DecodePayload(raw)
		if err != nil {
			m.log.Error("perago: callback payload", "err", err)
			continue
		}
		job, err := m.decodeJob(ctx, p, false)
		if err != nil {
			m.log.Error("perago: callback decode", "job", p.Job, "err", err)
			continue
		}
		if err := safeHandle(ctx, job); err != nil {
			m.log.Error("perago: callback failed", "job", p.Job, "err", err)
		}
	}
}

// chainedBatch is the job that stands for a batch inside a chain (Laravel's ChainedBatch).
type chainedBatch struct {
	Name    string            `json:"name,omitempty"`
	Items   []json.RawMessage `json:"items"`
	Count   int               `json:"count"`
	Options BatchOptions      `json:"options"`
}

func (*chainedBatch) JobName() string       { return "perago.ChainedBatch" }
func (c *chainedBatch) DisplayName() string { return "Batch " + c.Name }

func (c *chainedBatch) Handle(ctx context.Context) error {
	rt, ok := JobFrom(ctx)
	if !ok {
		return ErrNotInJob
	}
	opts := c.Options
	rt.mu.Lock()
	opts.Chain, rt.payload.Chain = rt.payload.Chain, nil
	opts.ChainConnection, opts.ChainQueue, opts.ChainCatch = rt.payload.ChainConnection, rt.payload.ChainQueue, rt.payload.ChainCatch
	rt.mu.Unlock()
	_, err := rt.m.dispatchBatch(ctx, c.Name, opts, c.Items, c.Count, nil)
	return err
}

func (p *PendingBatch) toChained() (*chainedBatch, error) {
	conn, queue, err := p.target()
	if err != nil {
		return nil, err
	}
	opts, err := p.options(conn, queue)
	if err != nil {
		return nil, err
	}
	raws, count, err := p.m.encodeBatchItems(p.jobs, p.conn, p.queue)
	if err != nil {
		return nil, err
	}
	return &chainedBatch{Name: p.name, Items: raws, Count: count, Options: opts}, nil
}

func init() {
	RegisterFactory("perago.ChainedBatch", func() Job { return &chainedBatch{} })
}

// MemoryBatchStore keeps batches in memory (tests, single-process apps).
type MemoryBatchStore struct {
	mu sync.Mutex
	m  map[string]*Batch
}

// NewMemoryBatchStore returns an empty in-memory batch store.
func NewMemoryBatchStore() *MemoryBatchStore { return &MemoryBatchStore{m: map[string]*Batch{}} }

func (s *MemoryBatchStore) copyOf(b *Batch) *Batch {
	c := *b
	c.FailedJobIDs = append([]string(nil), b.FailedJobIDs...)
	c.m, c.recorder = nil, nil
	return &c
}

func (s *MemoryBatchStore) get(id string) (*Batch, error) {
	b, ok := s.m[id]
	if !ok {
		return nil, Newf(BatchNotFound, "batch %s not found", id)
	}
	return b, nil
}

func (s *MemoryBatchStore) Store(_ context.Context, b *Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[b.ID] = s.copyOf(b)
	return nil
}

func (s *MemoryBatchStore) Find(_ context.Context, id string) (*Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.get(id)
	if err != nil {
		return nil, err
	}
	return s.copyOf(b), nil
}

func (s *MemoryBatchStore) List(_ context.Context, limit int, before string) ([]*Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := make([]*Batch, 0, len(s.m))
	for _, b := range s.m {
		all = append(all, s.copyOf(b))
	}
	sort.Slice(all, func(i, j int) bool { return all[i].CreatedAt.After(all[j].CreatedAt) })
	if before != "" {
		for i, b := range all {
			if b.ID == before {
				all = all[i+1:]
				break
			}
		}
	}
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

func (s *MemoryBatchStore) IncrementTotalJobs(_ context.Context, id string, n int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.get(id)
	if err != nil {
		return err
	}
	b.TotalJobs += n
	b.PendingJobs += n
	b.FinishedAt = nil
	return nil
}

func (s *MemoryBatchStore) RecordSuccess(_ context.Context, id, jobID string) (*Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.get(id)
	if err != nil {
		return nil, err
	}
	b.PendingJobs--
	b.FailedJobIDs = without(b.FailedJobIDs, jobID)
	return s.copyOf(b), nil
}

func (s *MemoryBatchStore) RecordFailure(_ context.Context, id, jobID string) (*Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.get(id)
	if err != nil {
		return nil, err
	}
	b.FailedJobs++
	b.FailedJobIDs = append(b.FailedJobIDs, jobID)
	return s.copyOf(b), nil
}

func (s *MemoryBatchStore) MarkFinished(_ context.Context, id string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.get(id)
	if err != nil {
		return err
	}
	b.FinishedAt = &at
	return nil
}

func (s *MemoryBatchStore) Cancel(_ context.Context, id string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.get(id)
	if err != nil {
		return err
	}
	b.CancelledAt, b.FinishedAt = &at, &at
	return nil
}

func (s *MemoryBatchStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, id)
	return nil
}

func (s *MemoryBatchStore) prune(match func(*Batch) bool) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for id, b := range s.m {
		if match(b) {
			delete(s.m, id)
			n++
		}
	}
	return n
}

func (s *MemoryBatchStore) Prune(_ context.Context, before time.Time) (int64, error) {
	return s.prune(func(b *Batch) bool { return b.FinishedAt != nil && b.FinishedAt.Before(before) }), nil
}

func (s *MemoryBatchStore) PruneUnfinished(_ context.Context, before time.Time) (int64, error) {
	return s.prune(func(b *Batch) bool { return b.FinishedAt == nil && b.CreatedAt.Before(before) }), nil
}

func (s *MemoryBatchStore) PruneCancelled(_ context.Context, before time.Time) (int64, error) {
	return s.prune(func(b *Batch) bool { return b.CancelledAt != nil && b.CancelledAt.Before(before) }), nil
}

func without(ids []string, id string) []string {
	out := ids[:0:0]
	for _, x := range ids {
		if x != id {
			out = append(out, x)
		}
	}
	return out
}

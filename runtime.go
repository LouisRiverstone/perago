package perago

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// Runtime is a job being processed (Laravel's Illuminate\Contracts\Queue\Job).
// Jobs reach it through the embedded Queueable or JobFrom(ctx); middleware through JobFrom.
type Runtime struct {
	m          *Manager
	connection string
	reserved   Reserved
	payload    *Payload
	job        Job

	mu           sync.Mutex
	released     bool
	releaseDelay time.Duration
	deleted      bool
	failed       bool
	failErr      error
	chainChanged bool

	backoff []time.Duration // the worker's --backoff
	batch   *Batch          // set by fakes
	fake    bool
}

type runtimeKey struct{}
type errorKey struct{}
type batchKey struct{}

// JobFrom returns the job runtime of a context passed to Handle or to a middleware.
func JobFrom(ctx context.Context) (*Runtime, bool) {
	rt, ok := ctx.Value(runtimeKey{}).(*Runtime)
	return rt, ok
}

// ErrorFrom returns the error a failure callback (chain Catch, batch Catch) was called for.
func ErrorFrom(ctx context.Context) error {
	err, _ := ctx.Value(errorKey{}).(error)
	return err
}

// BatchFrom returns the batch a batch callback (Then, Catch, Finally, Progress) was called for.
func BatchFrom(ctx context.Context) *Batch {
	b, _ := ctx.Value(batchKey{}).(*Batch)
	return b
}

func (r *Runtime) bind(ctx context.Context) context.Context {
	if q, ok := r.job.(queueableJob); ok {
		q.queueable().rt = r
	}
	return context.WithValue(ctx, runtimeKey{}, r)
}

// Manager is the manager processing the job.
func (r *Runtime) Manager() *Manager { return r.m }

// Job is the decoded job (nil when it could not be decoded).
func (r *Runtime) Job() Job { return r.job }

// Payload is the job's envelope.
func (r *Runtime) Payload() *Payload { return r.payload }

// UUID is the job's UUID.
func (r *Runtime) UUID() string { return r.payload.UUID }

// ID is the driver's id for the job.
func (r *Runtime) ID() string {
	if r.reserved == nil {
		return r.payload.UUID
	}
	return r.reserved.ID()
}

// Name is the registered job name.
func (r *Runtime) Name() string { return r.payload.Job }

// DisplayName is the job's display name.
func (r *Runtime) DisplayName() string { return r.payload.DisplayName }

// Connection is the connection the job was popped from.
func (r *Runtime) Connection() string { return r.connection }

// Queue is the queue the job was popped from.
func (r *Runtime) Queue() string {
	if r.reserved == nil {
		return r.payload.Queue
	}
	return r.reserved.Queue()
}

// Attempts is how many times the job has been attempted, this attempt included.
func (r *Runtime) Attempts() int {
	if r.reserved == nil {
		return 1
	}
	return r.reserved.Attempts()
}

// MaxTries is the job's resolved tries (0 = unlimited).
func (r *Runtime) MaxTries(fallback int) int {
	if r.payload.MaxTries != nil {
		return *r.payload.MaxTries
	}
	return fallback
}

// Release puts the job back on the queue after delay.
func (r *Runtime) Release(delay time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.released, r.releaseDelay = true, delay
}

// ReleaseUsingBackoff releases the job with the delay of its backoff for this attempt.
func (r *Runtime) ReleaseUsingBackoff() {
	r.Release(r.payload.backoffFor(r.Attempts(), r.backoff))
}

// Delete removes the job from the queue.
func (r *Runtime) Delete() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleted = true
}

// Fail marks the job as failed with err.
func (r *Runtime) Fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err == nil {
		err = Newf(JobFailedManually, "the job was marked as failed")
	}
	r.failed, r.failErr = true, err
}

// IsReleased reports whether the job was released, and with what delay.
func (r *Runtime) IsReleased() (bool, time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.released, r.releaseDelay
}

// IsDeleted reports whether the job deleted itself.
func (r *Runtime) IsDeleted() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.deleted
}

// HasFailed reports whether the job was marked as failed, and why.
func (r *Runtime) HasFailed() (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failed, r.failErr
}

// Batch loads the job's batch, nil when it is not batched.
func (r *Runtime) Batch(ctx context.Context) (*Batch, error) {
	if r.batch != nil {
		return r.batch, nil
	}
	if r.payload.BatchID == "" {
		return nil, nil
	}
	return r.m.FindBatch(ctx, r.payload.BatchID)
}

// Chain is the payloads still to run after this job.
func (r *Runtime) Chain() []*Payload {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*Payload, 0, len(r.payload.Chain))
	for _, raw := range r.payload.Chain {
		if p, err := DecodePayload(raw); err == nil {
			out = append(out, p)
		}
	}
	return out
}

// PrependToChain runs job right after this one.
func (r *Runtime) PrependToChain(job Job) error { return r.addToChain(job, true) }

// AppendToChain runs job at the end of this job's chain.
func (r *Runtime) AppendToChain(job Job) error { return r.addToChain(job, false) }

func (r *Runtime) addToChain(job Job, front bool) error {
	conn, queue := r.payload.ChainConnection, r.payload.ChainQueue
	if conn == "" {
		conn = r.payload.Connection
	}
	if queue == "" {
		queue = r.payload.Queue
	}
	var raw json.RawMessage
	if r.fake {
		b, err := json.Marshal(fakeChainEntry{Job: NameOf(job)})
		if err != nil {
			return Wrap(PayloadInvalid, err, "chain")
		}
		raw = b
	}
	if !r.fake {
		p, err := r.m.buildPayload(&PendingDispatch{m: r.m, job: job}, conn, queue, r.m.isLocal(conn))
		if err != nil {
			return err
		}
		p.BatchID = r.payload.BatchID
		b, err := p.encode()
		if err != nil {
			return err
		}
		raw = b
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chainChanged = true
	if front {
		r.payload.Chain = append([]json.RawMessage{raw}, r.payload.Chain...)
		return nil
	}
	r.payload.Chain = append(r.payload.Chain, raw)
	return nil
}

type fakeChainEntry struct {
	Job string `json:"job"`
}

// NewFakeRuntime binds job to a runtime that only records what the job does with the queue
// (release, delete, fail, chain changes). queuetest.WithFakeQueueInteractions uses it.
func NewFakeRuntime(job Job, attempts int) *Runtime {
	rt := &Runtime{
		job:      job,
		fake:     true,
		payload:  &Payload{UUID: newUUID(), Job: NameOf(job), DisplayName: DisplayNameOf(job)},
		reserved: &memoryReserved{attempts: max(attempts, 1)},
	}
	if q, ok := job.(queueableJob); ok {
		q.queueable().rt = rt
	}
	return rt
}

// SetFakeBatch makes Batch return b (queuetest.WithFakeBatch).
func (r *Runtime) SetFakeBatch(b *Batch) {
	r.batch = b
	r.payload.BatchID = b.ID
}

// BindRuntime returns ctx carrying rt, as the worker passes it to Handle (queuetest uses it).
func BindRuntime(ctx context.Context, rt *Runtime) context.Context { return rt.bind(ctx) }

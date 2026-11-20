package perago

import (
	"context"
	"errors"
	"time"
)

// Queueable gives a job Laravel's Queueable, InteractsWithQueue and Batchable methods.
// Embed it in the job struct; it is never serialized.
//
//	type ProcessPodcast struct {
//		perago.Queueable
//		PodcastID int
//	}
//
// The OnQueue/OnConnection/... setters are defaults the dispatch may override. The other methods
// work while the job is being handled.
type Queueable struct {
	rt *Runtime

	connection  string
	queue       string
	delay       *time.Duration
	afterCommit *bool
	group       string
}

type queueableJob interface{ queueable() *Queueable }

func (q *Queueable) queueable() *Queueable { return q }

// OnQueue sets the job's default queue.
func (q *Queueable) OnQueue(name string) { q.queue = name }

// OnConnection sets the job's default connection.
func (q *Queueable) OnConnection(name string) { q.connection = name }

// DelayFor sets the job's default delay.
func (q *Queueable) DelayFor(d time.Duration) { q.delay = &d }

// AfterCommit makes the job wait for the surrounding Manager.Transaction to commit.
func (q *Queueable) AfterCommit() { t := true; q.afterCommit = &t }

// BeforeCommit dispatches the job right away even on an after_commit connection.
func (q *Queueable) BeforeCommit() { f := false; q.afterCommit = &f }

// OnGroup sets the SQS message group.
func (q *Queueable) OnGroup(group string) { q.group = group }

// Job is the job's runtime while it runs, nil otherwise.
func (q *Queueable) Job() *Runtime { return q.rt }

// Attempts is how many times the job has been attempted, this attempt included.
func (q *Queueable) Attempts() int {
	if q.rt == nil {
		return 1
	}
	return q.rt.Attempts()
}

// Release puts the job back on the queue, available after delay (0 when omitted).
func (q *Queueable) Release(delay ...time.Duration) {
	if q.rt != nil {
		q.rt.Release(firstOr(delay, 0))
	}
}

// ReleaseUsingBackoff releases the job with the delay its backoff sets for this attempt.
func (q *Queueable) ReleaseUsingBackoff() {
	if q.rt != nil {
		q.rt.ReleaseUsingBackoff()
	}
}

// Delete removes the job from the queue; a chain still continues.
func (q *Queueable) Delete() {
	if q.rt != nil {
		q.rt.Delete()
	}
}

// Fail marks the job as failed. reason is an error, a string, or nothing.
func (q *Queueable) Fail(reason ...any) {
	if q.rt != nil {
		q.rt.Fail(reasonError(reason))
	}
}

// JobID is the job's UUID.
func (q *Queueable) JobID() string {
	if q.rt == nil {
		return ""
	}
	return q.rt.UUID()
}

// BatchID is the id of the job's batch, "" when it is not batched.
func (q *Queueable) BatchID() string {
	if q.rt == nil {
		return ""
	}
	return q.rt.payload.BatchID
}

// Batch loads the job's batch, nil when it is not batched.
func (q *Queueable) Batch(ctx context.Context) (*Batch, error) {
	if q.rt == nil {
		return nil, nil
	}
	return q.rt.Batch(ctx)
}

// PrependToChain runs job right after this one.
func (q *Queueable) PrependToChain(job Job) error {
	if q.rt == nil {
		return ErrNotInJob
	}
	return q.rt.PrependToChain(job)
}

// AppendToChain runs job at the end of this job's chain.
func (q *Queueable) AppendToChain(job Job) error {
	if q.rt == nil {
		return ErrNotInJob
	}
	return q.rt.AppendToChain(job)
}

func reasonError(reason []any) error {
	if len(reason) == 0 || reason[0] == nil {
		return Newf(JobFailedManually, "the job was marked as failed")
	}
	switch r := reason[0].(type) {
	case error:
		return r
	case string:
		return errors.New(r)
	}
	return Newf(JobFailedManually, "%v", reason[0])
}

func firstOr[T any](vs []T, def T) T {
	if len(vs) == 0 {
		return def
	}
	return vs[0]
}

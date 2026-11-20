package perago

import (
	"context"
	"reflect"
	"sync"
	"time"
)

// Queue events (Illuminate\Queue\Events\*). Listen with perago.Listen[E](m, fn) or the
// Before/After/Failing/Looping shortcuts.
type (
	JobQueueing struct {
		Connection, Queue string
		Job               Job
		Payload           *Payload
		Delay             time.Duration
	}
	JobQueued struct {
		Connection, Queue string
		ID                string
		Job               Job
		Payload           *Payload
		Delay             time.Duration
	}
	JobPopping struct{ Connection string }
	JobPopped  struct {
		Connection string
		Job        *Runtime // nil when the queues were empty
	}
	JobProcessing struct {
		Connection string
		Job        *Runtime
	}
	JobProcessed struct {
		Connection string
		Job        *Runtime
	}
	// JobAttempted fires after every attempt, successful or not.
	JobAttempted struct {
		Connection        string
		Job               *Runtime
		ExceptionOccurred bool
	}
	JobExceptionOccurred struct {
		Connection string
		Job        *Runtime
		Err        error
	}
	JobReleasedAfterException struct {
		Connection string
		Job        *Runtime
		Delay      time.Duration
	}
	JobFailed struct {
		Connection string
		Job        *Runtime
		Err        error
	}
	JobTimedOut struct {
		Connection string
		Job        *Runtime
	}
	JobRetryRequested struct{ Failed FailedJob }
	JobDebounced      struct {
		Connection string
		Job        *Runtime
	}
	Looping struct {
		Connection string
		Queues     []string
	}
	WorkerStarting struct {
		Connection string
		Queues     []string
		Options    WorkerOptions
	}
	WorkerStopping struct{ Status int }
	WorkerIdle     struct {
		Connection string
		Queues     []string
	}
	QueueBusy struct {
		Connection, Queue string
		Size              int64
	}
	QueueFailedOver struct {
		Connection string // the connection that failed
		Err        error
	}
	QueuePaused struct {
		Connection, Queue string
		TTL               time.Duration
	}
	QueueResumed    struct{ Connection, Queue string }
	BatchDispatched struct{ Batch *Batch }
)

type listener struct {
	id uint64
	fn func(ctx context.Context, e any) bool
}

type eventBus struct {
	mu   sync.RWMutex
	next uint64
	subs map[reflect.Type][]listener
}

func (b *eventBus) add(t reflect.Type, fn func(ctx context.Context, e any) bool) func() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subs == nil {
		b.subs = map[reflect.Type][]listener{}
	}
	b.next++
	id := b.next
	b.subs[t] = append(b.subs[t], listener{id: id, fn: fn})
	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		ls := b.subs[t]
		for i, l := range ls {
			if l.id == id {
				b.subs[t] = append(ls[:i:i], ls[i+1:]...)
				return
			}
		}
	}
}

// emit runs every listener of e's type in order; it reports false when any returned false.
func (b *eventBus) emit(ctx context.Context, e any) bool {
	b.mu.RLock()
	ls := append([]listener(nil), b.subs[reflect.TypeOf(e)]...)
	b.mu.RUnlock()
	ok := true
	for _, l := range ls {
		if !l.fn(ctx, e) {
			ok = false
		}
	}
	return ok
}

// Listen subscribes fn to events of type E and returns the unsubscribe function.
//
//	perago.Listen(m, func(ctx context.Context, e perago.JobFailed) { log.Println(e.Err) })
func Listen[E any](m *Manager, fn func(ctx context.Context, e E)) (off func()) {
	return m.events.add(reflect.TypeFor[E](), func(ctx context.Context, e any) bool {
		fn(ctx, e.(E))
		return true
	})
}

// Before runs fn before each job (Queue::before).
func (m *Manager) Before(fn func(ctx context.Context, e JobProcessing)) func() {
	return Listen(m, fn)
}

// After runs fn after each successful job (Queue::after).
func (m *Manager) After(fn func(ctx context.Context, e JobProcessed)) func() {
	return Listen(m, fn)
}

// Failing runs fn when a job fails (Queue::failing).
func (m *Manager) Failing(fn func(ctx context.Context, e JobFailed)) func() {
	return Listen(m, fn)
}

// Looping runs fn before the worker pops a job (Queue::looping); returning false makes the
// worker skip this iteration.
func (m *Manager) Looping(fn func(ctx context.Context, e Looping) bool) func() {
	return m.events.add(reflect.TypeFor[Looping](), func(ctx context.Context, e any) bool {
		return fn(ctx, e.(Looping))
	})
}

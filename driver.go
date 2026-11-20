package perago

import (
	"context"
	"sync"
	"time"
)

// Message is one job handed to a driver.
type Message struct {
	ID              string // the payload UUID; drivers may use it as their job id
	Queue           string
	Payload         []byte
	Delay           time.Duration
	Group           string // SQS message group (FIFO and fair queues)
	DeduplicationID string // SQS FIFO deduplication id
}

// Reserved is a job a worker popped. Attempts counts reservations, this one included.
type Reserved interface {
	ID() string
	Queue() string
	Payload() []byte
	Attempts() int
	Delete(ctx context.Context) error
	Release(ctx context.Context, delay time.Duration) error
}

// Driver is a queue backend (Laravel's Queue contract).
type Driver interface {
	Push(ctx context.Context, m Message) (id string, err error)
	// Pop reserves the next available job, or returns nil, nil when the queue is empty.
	Pop(ctx context.Context, queue string) (Reserved, error)
	// Size counts every job of the queue: pending, delayed and reserved.
	Size(ctx context.Context, queue string) (int64, error)
	// Clear deletes every job of the queue and returns how many were deleted.
	Clear(ctx context.Context, queue string) (int64, error)
}

// Optional driver capabilities.
type (
	// BulkPusher pushes many messages in one round trip (Bus::bulk).
	BulkPusher interface {
		PushBulk(ctx context.Context, ms []Message) ([]string, error)
	}
	// SizeReporter breaks Size down (Queue::pendingSize, delayedSize, reservedSize).
	SizeReporter interface {
		PendingSize(ctx context.Context, queue string) (int64, error)
		DelayedSize(ctx context.Context, queue string) (int64, error)
		ReservedSize(ctx context.Context, queue string) (int64, error)
	}
	// AgeReporter knows when the oldest pending job was created (zero when none), for
	// queue:monitor.
	AgeReporter interface {
		OldestPending(ctx context.Context, queue string) (time.Time, error)
	}
	// Closer releases driver resources when the Manager closes.
	Closer interface{ Close() error }
)

// Null discards every job (the null driver).
func Null() Driver { return nullDriver{} }

type nullDriver struct{}

func (nullDriver) Push(context.Context, Message) (string, error) { return "", nil }
func (nullDriver) Pop(context.Context, string) (Reserved, error) { return nil, nil }
func (nullDriver) Size(context.Context, string) (int64, error)   { return 0, nil }
func (nullDriver) Clear(context.Context, string) (int64, error)  { return 0, nil }

type localKind int

const (
	kindSync localKind = iota + 1
	kindDeferred
	kindBackground
	kindFailover
)

// localDriver marks the connections the Manager runs itself.
type localDriver struct {
	kind  localKind
	conns []string
}

func (localDriver) Push(context.Context, Message) (string, error) {
	return "", Newf(InvalidConfig, "in-process connections are run by the Manager")
}
func (d localDriver) Pop(context.Context, string) (Reserved, error) {
	return nil, Newf(InvalidConfig, "%s connections have no worker", d.name())
}
func (localDriver) Size(context.Context, string) (int64, error)  { return 0, nil }
func (localDriver) Clear(context.Context, string) (int64, error) { return 0, nil }

func (d localDriver) name() string {
	switch d.kind {
	case kindSync:
		return "sync"
	case kindDeferred:
		return "deferred"
	case kindBackground:
		return "background"
	}
	return "failover"
}

// SyncDriver runs each job immediately in the dispatching goroutine (the sync driver).
// A failing job's error is returned to the caller.
func SyncDriver() Driver { return localDriver{kind: kindSync} }

// DeferredDriver runs jobs in this process after the current request (the deferred driver):
// when the dispatch context carries a scope from WithDeferred or Manager.Middleware, at the end
// of that scope; otherwise right away in a background goroutine.
func DeferredDriver() Driver { return localDriver{kind: kindDeferred} }

// BackgroundDriver runs each job in its own goroutine (the background driver).
// Manager.Wait and Manager.Close wait for them.
func BackgroundDriver() Driver { return localDriver{kind: kindBackground} }

// FailoverDriver pushes to the first connection that accepts the job (the failover driver),
// emitting QueueFailedOver for each one that fails. Run a worker per underlying connection.
func FailoverDriver(connections ...string) Driver {
	return localDriver{kind: kindFailover, conns: connections}
}

// memoryReserved runs a payload in-process without a backing queue.
type memoryReserved struct {
	mu       sync.Mutex
	id       string
	queue    string
	payload  []byte
	attempts int
	released *time.Duration
	deleted  bool
}

func (r *memoryReserved) ID() string      { return r.id }
func (r *memoryReserved) Queue() string   { return r.queue }
func (r *memoryReserved) Payload() []byte { return r.payload }
func (r *memoryReserved) Attempts() int   { return r.attempts }
func (r *memoryReserved) Delete(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleted = true
	return nil
}
func (r *memoryReserved) Release(_ context.Context, d time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.released = &d
	return nil
}

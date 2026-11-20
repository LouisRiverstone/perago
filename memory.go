package perago

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemoryDriver is a complete queue kept in this process: delays, reservations and retry_after
// behave like the database driver. Useful for tests and single-process apps.
type MemoryDriver struct {
	mu         sync.Mutex
	jobs       map[string][]*memoryJob
	retryAfter time.Duration
	now        func() time.Time
}

type memoryJob struct {
	id          string
	payload     []byte
	attempts    int
	availableAt time.Time
	reservedAt  *time.Time
	createdAt   time.Time
}

// NewMemoryDriver returns an empty memory queue; reserved jobs come back after retryAfter
// (90s when 0).
func NewMemoryDriver(retryAfter time.Duration) *MemoryDriver {
	if retryAfter <= 0 {
		retryAfter = 90 * time.Second
	}
	return &MemoryDriver{jobs: map[string][]*memoryJob{}, retryAfter: retryAfter, now: time.Now}
}

// InProcess reports true: jobs never leave the process, so closures are allowed.
func (d *MemoryDriver) InProcess() bool { return true }

// SetClock replaces the driver's clock (tests).
func (d *MemoryDriver) SetClock(now func() time.Time) { d.now = now }

func (d *MemoryDriver) Push(_ context.Context, m Message) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	id := m.ID
	if id == "" {
		id = newUUID()
	}
	now := d.now()
	d.jobs[m.Queue] = append(d.jobs[m.Queue], &memoryJob{id: id, payload: m.Payload, availableAt: now.Add(m.Delay), createdAt: now})
	return id, nil
}

func (d *MemoryDriver) PushBulk(ctx context.Context, ms []Message) ([]string, error) {
	ids := make([]string, 0, len(ms))
	for _, m := range ms {
		id, err := d.Push(ctx, m)
		if err != nil {
			return ids, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (d *MemoryDriver) Pop(_ context.Context, queue string) (Reserved, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	for _, j := range d.jobs[queue] {
		free := j.reservedAt == nil && !j.availableAt.After(now)
		expired := j.reservedAt != nil && !j.reservedAt.Add(d.retryAfter).After(now)
		if !free && !expired {
			continue
		}
		j.reservedAt = &now
		j.attempts++
		return &memoryHandle{d: d, queue: queue, job: j, attempts: j.attempts}, nil
	}
	return nil, nil
}

func (d *MemoryDriver) Size(_ context.Context, queue string) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return int64(len(d.jobs[queue])), nil
}

func (d *MemoryDriver) Clear(_ context.Context, queue string) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := int64(len(d.jobs[queue]))
	delete(d.jobs, queue)
	return n, nil
}

func (d *MemoryDriver) count(queue string, match func(*memoryJob, time.Time) bool) int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	var n int64
	for _, j := range d.jobs[queue] {
		if match(j, now) {
			n++
		}
	}
	return n
}

func (d *MemoryDriver) PendingSize(_ context.Context, queue string) (int64, error) {
	return d.count(queue, func(j *memoryJob, now time.Time) bool { return j.reservedAt == nil && !j.availableAt.After(now) }), nil
}

func (d *MemoryDriver) DelayedSize(_ context.Context, queue string) (int64, error) {
	return d.count(queue, func(j *memoryJob, now time.Time) bool { return j.reservedAt == nil && j.availableAt.After(now) }), nil
}

func (d *MemoryDriver) ReservedSize(_ context.Context, queue string) (int64, error) {
	return d.count(queue, func(j *memoryJob, _ time.Time) bool { return j.reservedAt != nil }), nil
}

func (d *MemoryDriver) OldestPending(_ context.Context, queue string) (time.Time, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var ts []time.Time
	for _, j := range d.jobs[queue] {
		if j.reservedAt == nil {
			ts = append(ts, j.createdAt)
		}
	}
	if len(ts) == 0 {
		return time.Time{}, nil
	}
	sort.Slice(ts, func(i, k int) bool { return ts[i].Before(ts[k]) })
	return ts[0], nil
}

// Payloads returns the payloads of a queue in order (tests).
func (d *MemoryDriver) Payloads(queue string) [][]byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([][]byte, 0, len(d.jobs[queue]))
	for _, j := range d.jobs[queue] {
		out = append(out, j.payload)
	}
	return out
}

func (d *MemoryDriver) remove(queue string, target *memoryJob) {
	js := d.jobs[queue]
	for i, j := range js {
		if j == target {
			d.jobs[queue] = append(js[:i:i], js[i+1:]...)
			return
		}
	}
}

type memoryHandle struct {
	d        *MemoryDriver
	queue    string
	job      *memoryJob
	attempts int
}

func (h *memoryHandle) ID() string      { return h.job.id }
func (h *memoryHandle) Queue() string   { return h.queue }
func (h *memoryHandle) Payload() []byte { return h.job.payload }
func (h *memoryHandle) Attempts() int   { return h.attempts }

func (h *memoryHandle) Delete(context.Context) error {
	h.d.mu.Lock()
	defer h.d.mu.Unlock()
	h.d.remove(h.queue, h.job)
	return nil
}

func (h *memoryHandle) Release(_ context.Context, delay time.Duration) error {
	h.d.mu.Lock()
	defer h.d.mu.Unlock()
	h.job.reservedAt = nil
	h.job.availableAt = h.d.now().Add(delay)
	return nil
}

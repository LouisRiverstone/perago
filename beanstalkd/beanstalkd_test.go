package beanstalkd_test

import (
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/beanstalkd"
	"github.com/LouisRiverstone/perago/internal/drivertest"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type fjob struct {
	id            uint64
	tube          string
	body          []byte
	readyAt       time.Time
	reservedUntil *time.Time
	reserves      int
}

// fakeConn mimics beanstalkd's ready/delayed/reserved states and TTR.
type fakeConn struct {
	mu    sync.Mutex
	clock *clock
	n     uint64
	jobs  map[uint64]*fjob
	ttr   map[uint64]time.Duration
}

func newFake(c *clock) *fakeConn {
	return &fakeConn{clock: c, jobs: map[uint64]*fjob{}, ttr: map[uint64]time.Duration{}}
}

func (f *fakeConn) expire() {
	now := f.clock.Now()
	for _, j := range f.jobs {
		if j.reservedUntil != nil && !j.reservedUntil.After(now) {
			j.reservedUntil = nil
			j.readyAt = now
		}
	}
}

func (f *fakeConn) Put(tube string, body []byte, _ uint32, delay, ttr time.Duration) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	f.jobs[f.n] = &fjob{id: f.n, tube: tube, body: body, readyAt: f.clock.Now().Add(delay)}
	f.ttr[f.n] = ttr
	return f.n, nil
}

func (f *fakeConn) ordered() []*fjob {
	out := make([]*fjob, 0, len(f.jobs))
	for _, j := range f.jobs {
		out = append(out, j)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].id < out[k].id })
	return out
}

func (f *fakeConn) Reserve(tube string, _ time.Duration) (uint64, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.expire()
	now := f.clock.Now()
	for _, j := range f.ordered() {
		if j.tube != tube || j.reservedUntil != nil || j.readyAt.After(now) {
			continue
		}
		until := now.Add(f.ttr[j.id])
		j.reservedUntil = &until
		j.reserves++
		return j.id, j.body, nil
	}
	return 0, nil, beanstalkd.ErrTimeout
}

func (f *fakeConn) Delete(id uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.jobs[id]; !ok {
		return beanstalkd.ErrNotFound
	}
	delete(f.jobs, id)
	return nil
}

func (f *fakeConn) Release(id uint64, _ uint32, delay time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j := f.jobs[id]
	j.reservedUntil = nil
	j.readyAt = f.clock.Now().Add(delay)
	return nil
}

func (f *fakeConn) StatsJob(id uint64) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return map[string]string{"reserves": strconv.Itoa(f.jobs[id].reserves)}, nil
}

func (f *fakeConn) StatsTube(tube string) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.expire()
	now := f.clock.Now()
	var ready, delayed, reserved int
	for _, j := range f.jobs {
		if j.tube != tube {
			continue
		}
		switch {
		case j.reservedUntil != nil:
			reserved++
		case j.readyAt.After(now):
			delayed++
		default:
			ready++
		}
	}
	return map[string]string{"current-jobs-ready": strconv.Itoa(ready), "current-jobs-delayed": strconv.Itoa(delayed), "current-jobs-reserved": strconv.Itoa(reserved)}, nil
}

func (f *fakeConn) Peek(tube, state string) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.clock.Now()
	for _, j := range f.ordered() {
		if j.tube != tube || j.reservedUntil != nil {
			continue
		}
		if (state == "ready" && !j.readyAt.After(now)) || (state == "delayed" && j.readyAt.After(now)) {
			return j.id, nil
		}
	}
	return 0, beanstalkd.ErrNotFound
}

func (f *fakeConn) Close() error { return nil }

func TestDriverConformance(t *testing.T) {
	drivertest.Run(t, func(t *testing.T) (perago.Driver, func(time.Duration)) {
		c := &clock{t: time.Unix(1_800_000_000, 0)}
		return beanstalkd.NewDriver(newFake(c), beanstalkd.Options{TTR: 5 * time.Second}), c.Advance
	}, 5*time.Second)
}

// TestLiveServer runs the suite on a real server when BEANSTALKD_ADDR is set (CI).
func TestLiveServer(t *testing.T) {
	addr := os.Getenv("BEANSTALKD_ADDR")
	if addr == "" {
		t.Skip("BEANSTALKD_ADDR not set")
	}
	drivertest.Run(t, func(t *testing.T) (perago.Driver, func(time.Duration)) {
		c, err := beanstalkd.Dial(addr)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		d := beanstalkd.NewDriver(c, beanstalkd.Options{TTR: 2 * time.Second})
		for _, q := range []string{"q1", "a", "b", "delayed", "rel", "exp", "clr", "bulk", "sz"} {
			_, _ = d.Clear(t.Context(), q)
		}
		return d, nil
	}, 2*time.Second)
}

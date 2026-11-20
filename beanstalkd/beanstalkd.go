// Package beanstalkd is perago's Beanstalkd queue driver. A queue is a tube; the job's TTR plays
// the part of retry_after and attempts come from the job's "reserves" statistic.
package beanstalkd

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/LouisRiverstone/perago"
	"github.com/beanstalkd/go-beanstalk"
)

// Conn is the part of a beanstalkd connection the driver needs; Dial returns one.
type Conn interface {
	Put(tube string, body []byte, pri uint32, delay, ttr time.Duration) (uint64, error)
	// Reserve waits up to timeout for a job of tube; it returns ErrTimeout when none came.
	Reserve(tube string, timeout time.Duration) (uint64, []byte, error)
	Delete(id uint64) error
	Release(id uint64, pri uint32, delay time.Duration) error
	StatsJob(id uint64) (map[string]string, error)
	StatsTube(tube string) (map[string]string, error)
	// Peek returns the next job of tube in state "ready", "delayed" or "buried", or ErrNotFound.
	Peek(tube, state string) (uint64, error)
	Close() error
}

// ErrTimeout and ErrNotFound are what Conn implementations return for an empty reserve or peek.
var (
	ErrTimeout  = beanstalk.ErrTimeout
	ErrNotFound = beanstalk.ErrNotFound
)

// Dial connects to a beanstalkd server.
func Dial(addr string) (Conn, error) {
	c, err := beanstalk.Dial("tcp", addr)
	if err != nil {
		return nil, perago.Wrap(perago.DriverFailed, err, "beanstalkd dial %s", addr)
	}
	return &conn{c: c}, nil
}

// conn serializes calls: a beanstalkd connection speaks one command at a time.
type conn struct {
	mu sync.Mutex
	c  *beanstalk.Conn
}

func (c *conn) Put(tube string, body []byte, pri uint32, delay, ttr time.Duration) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return beanstalk.NewTube(c.c, tube).Put(body, pri, delay, ttr)
}

func (c *conn) Reserve(tube string, timeout time.Duration) (uint64, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return beanstalk.NewTubeSet(c.c, tube).Reserve(timeout)
}

func (c *conn) Delete(id uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.c.Delete(id)
}

func (c *conn) Release(id uint64, pri uint32, delay time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.c.Release(id, pri, delay)
}

func (c *conn) StatsJob(id uint64) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.c.StatsJob(id)
}

func (c *conn) StatsTube(tube string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return beanstalk.NewTube(c.c, tube).Stats()
}

func (c *conn) Peek(tube, state string) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := beanstalk.NewTube(c.c, tube)
	var id uint64
	var err error
	switch state {
	case "ready":
		id, _, err = t.PeekReady()
	case "delayed":
		id, _, err = t.PeekDelayed()
	default:
		id, _, err = t.PeekBuried()
	}
	return id, err
}

func (c *conn) Close() error { return c.c.Close() }

// Options configures NewDriver.
type Options struct {
	// TTR is the time to run of each job (retry_after, default 90s).
	TTR time.Duration
	// Priority of pushed and released jobs (default 1024; lower runs first).
	Priority uint32
	// BlockFor makes Pop wait for a job up to this long (block_for). 0 polls.
	BlockFor time.Duration
}

// Driver is the Beanstalkd queue driver.
type Driver struct {
	c Conn
	o Options
}

// NewDriver returns the Beanstalkd queue driver over c.
func NewDriver(c Conn, o Options) *Driver {
	if o.TTR <= 0 {
		o.TTR = 90 * time.Second
	}
	if o.Priority == 0 {
		o.Priority = 1024
	}
	return &Driver{c: c, o: o}
}

func fail(err error, op string) error {
	if err == nil {
		return nil
	}
	return perago.Wrap(perago.DriverFailed, err, "beanstalkd %s", op)
}

func (d *Driver) Push(_ context.Context, m perago.Message) (string, error) {
	id, err := d.c.Put(m.Queue, m.Payload, d.o.Priority, m.Delay, d.o.TTR)
	if err != nil {
		return "", fail(err, "put")
	}
	return strconv.FormatUint(id, 10), nil
}

func (d *Driver) Pop(_ context.Context, queue string) (perago.Reserved, error) {
	id, body, err := d.c.Reserve(queue, d.o.BlockFor)
	if errors.Is(err, ErrTimeout) {
		return nil, nil
	}
	if err != nil {
		return nil, fail(err, "reserve")
	}
	stats, err := d.c.StatsJob(id)
	if err != nil {
		return nil, fail(err, "stats-job")
	}
	attempts, _ := strconv.Atoi(stats["reserves"])
	return &reserved{d: d, id: id, queue: queue, body: body, attempts: max(attempts, 1)}, nil
}

func (d *Driver) stat(queue, key string) (int64, error) {
	s, err := d.c.StatsTube(queue)
	if errors.Is(err, ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fail(err, "stats-tube")
	}
	n, _ := strconv.ParseInt(s[key], 10, 64)
	return n, nil
}

func (d *Driver) Size(_ context.Context, queue string) (int64, error) {
	var total int64
	for _, k := range []string{"current-jobs-ready", "current-jobs-delayed", "current-jobs-reserved"} {
		n, err := d.stat(queue, k)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func (d *Driver) PendingSize(_ context.Context, queue string) (int64, error) {
	return d.stat(queue, "current-jobs-ready")
}

func (d *Driver) DelayedSize(_ context.Context, queue string) (int64, error) {
	return d.stat(queue, "current-jobs-delayed")
}

func (d *Driver) ReservedSize(_ context.Context, queue string) (int64, error) {
	return d.stat(queue, "current-jobs-reserved")
}

// Clear deletes the ready, delayed and buried jobs of the tube (reserved ones belong to their
// workers).
func (d *Driver) Clear(_ context.Context, queue string) (int64, error) {
	var n int64
	for _, state := range []string{"ready", "delayed", "buried"} {
		for {
			id, err := d.c.Peek(queue, state)
			if errors.Is(err, ErrNotFound) {
				break
			}
			if err != nil {
				return n, fail(err, "peek")
			}
			if err := d.c.Delete(id); err != nil {
				return n, fail(err, "delete")
			}
			n++
		}
	}
	return n, nil
}

type reserved struct {
	d        *Driver
	id       uint64
	queue    string
	body     []byte
	attempts int
}

func (r *reserved) ID() string      { return strconv.FormatUint(r.id, 10) }
func (r *reserved) Queue() string   { return r.queue }
func (r *reserved) Payload() []byte { return r.body }
func (r *reserved) Attempts() int   { return r.attempts }

func (r *reserved) Delete(context.Context) error { return fail(r.d.c.Delete(r.id), "delete") }

func (r *reserved) Release(_ context.Context, delay time.Duration) error {
	return fail(r.d.c.Release(r.id, r.d.o.Priority, delay), "release")
}

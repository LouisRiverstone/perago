package perago

import (
	"context"
	"time"
)

// PendingChain is a chain being configured (Bus::chain).
type PendingChain struct {
	m     *Manager
	jobs  []any
	conn  string
	queue string
	catch []Job
	delay *time.Duration
}

// Chain runs jobs one after another; a job that fails stops the rest (Bus::chain).
// Items are Job values or *PendingBatch (a batch inside the chain).
func (m *Manager) Chain(jobs ...any) *PendingChain { return &PendingChain{m: m, jobs: jobs} }

// OnConnection sets the connection of every job of the chain that does not choose one.
func (c *PendingChain) OnConnection(name string) *PendingChain { c.conn = name; return c }

// OnQueue sets the queue of every job of the chain that does not choose one.
func (c *PendingChain) OnQueue(name string) *PendingChain { c.queue = name; return c }

// Delay delays the first job.
func (c *PendingChain) Delay(d time.Duration) *PendingChain { c.delay = &d; return c }

// Catch runs fn when a job of the chain fails. fn is a Go closure, so it only runs in this
// process; use CatchJob on connections served by other processes.
func (c *PendingChain) Catch(fn func(ctx context.Context, err error)) *PendingChain {
	return c.CatchJob(Closure(func(ctx context.Context) error { fn(ctx, ErrorFrom(ctx)); return nil }))
}

// CatchJob runs job inline when a job of the chain fails; ErrorFrom(ctx) is the failure.
func (c *PendingChain) CatchJob(job Job) *PendingChain { c.catch = append(c.catch, job); return c }

// Jobs is the chain's items.
func (c *PendingChain) Jobs() []any { return c.jobs }

// Dispatch queues the first job; the others travel in its payload.
func (c *PendingChain) Dispatch(ctx context.Context) (string, error) {
	if len(c.jobs) == 0 {
		return "", nil
	}
	first, err := chainItem(c.jobs[0])
	if err != nil {
		return "", err
	}
	pd := c.m.Dispatch(first)
	pd.connection, pd.queue = c.conn, c.queue
	pd.chain, pd.chainConn, pd.chainQueue, pd.catch = c.jobs[1:], c.conn, c.queue, c.catch
	pd.delay = c.delay
	return pd.Send(ctx)
}

func chainItem(item any) (Job, error) {
	switch v := item.(type) {
	case *PendingBatch:
		return v.toChained()
	case Job:
		return v, nil
	}
	return nil, Newf(InvalidConfig, "chain items must be jobs or batches, got %T", item)
}

// dispatchNextInChain pushes the job after p, carrying the rest of the chain.
func (m *Manager) dispatchNextInChain(ctx context.Context, p *Payload) error {
	if len(p.Chain) == 0 {
		return nil
	}
	next, err := DecodePayload(p.Chain[0])
	if err != nil {
		return err
	}
	next.Chain = p.Chain[1:]
	next.ChainConnection, next.ChainQueue, next.ChainCatch = p.ChainConnection, p.ChainQueue, p.ChainCatch
	if next.BatchID == "" {
		next.BatchID = p.BatchID
	}
	conn := next.Connection
	if _, ok := m.conns[conn]; !ok {
		conn = m.def
	}
	_, err = m.pushPayload(ctx, conn, next, time.Duration(next.Delay)*time.Millisecond)
	return err
}

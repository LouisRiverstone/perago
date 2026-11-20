package perago

import (
	"context"
	"time"
)

// Dispatched describes a dispatch to an Interceptor.
type Dispatched struct {
	Job           Job
	Connection    string
	Queue         string
	Delay         time.Duration
	Chain         []any
	Sync          bool
	AfterResponse bool
}

// Interceptor captures dispatches before they reach a driver; queuetest installs one.
// Returning false lets the dispatch through.
type Interceptor interface {
	InterceptDispatch(ctx context.Context, d Dispatched) bool
	InterceptBatch(ctx context.Context, b *PendingBatch) (*Batch, bool)
}

// Intercept installs i (nil removes it) and returns the function restoring the previous one.
func (m *Manager) Intercept(i Interceptor) (restore func()) {
	m.mu.Lock()
	prev := m.fake
	m.fake = i
	m.mu.Unlock()
	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.fake = prev
	}
}

func (m *Manager) interceptor() Interceptor {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.fake
}

func (m *Manager) intercept(ctx context.Context, pd *PendingDispatch, conn, queue string) (bool, string) {
	i := m.interceptor()
	if i == nil {
		return false, ""
	}
	d := Dispatched{Job: pd.job, Connection: conn, Queue: queue, Delay: m.delayOf(pd), Chain: pd.chain, Sync: pd.sync, AfterResponse: pd.afterResponse}
	if !i.InterceptDispatch(ctx, d) {
		return false, ""
	}
	return true, newUUID()
}

func (m *Manager) interceptBatch(ctx context.Context, p *PendingBatch) (*Batch, bool) {
	i := m.interceptor()
	if i == nil {
		return nil, false
	}
	return i.InterceptBatch(ctx, p)
}

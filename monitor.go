package perago

import (
	"context"
	"strings"
	"time"
)

// QueueStatus is one row of queue:monitor. The breakdown is -1 when the driver cannot report it.
type QueueStatus struct {
	Connection    string
	Queue         string
	Size          int64
	Pending       int64
	Delayed       int64
	Reserved      int64
	OldestPending time.Time
	Busy          bool
}

// Monitor reports the size of each target ("connection:queue", or a queue of the default
// connection) and fires QueueBusy for those holding max jobs or more (queue:monitor).
func (m *Manager) Monitor(ctx context.Context, targets []string, max int64) ([]QueueStatus, error) {
	out := make([]QueueStatus, 0, len(targets))
	for _, target := range targets {
		conn, queue, ok := strings.Cut(target, ":")
		if !ok {
			conn, queue = "", target
		}
		conn, queue = m.resolveName(conn, queue)
		c, err := m.Connection(conn)
		if err != nil {
			return nil, err
		}
		st := QueueStatus{Connection: conn, Queue: queue, Pending: -1, Delayed: -1, Reserved: -1}
		if st.Size, err = c.Driver.Size(ctx, queue); err != nil {
			return nil, err
		}
		if sr, ok := c.Driver.(SizeReporter); ok {
			st.Pending, _ = sr.PendingSize(ctx, queue)
			st.Delayed, _ = sr.DelayedSize(ctx, queue)
			st.Reserved, _ = sr.ReservedSize(ctx, queue)
		}
		if ar, ok := c.Driver.(AgeReporter); ok {
			st.OldestPending, _ = ar.OldestPending(ctx, queue)
		}
		st.Busy = max > 0 && st.Size >= max
		if st.Busy {
			m.events.emit(ctx, QueueBusy{Connection: conn, Queue: queue, Size: st.Size})
		}
		out = append(out, st)
	}
	return out, nil
}

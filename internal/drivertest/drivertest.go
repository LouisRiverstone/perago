// Package drivertest is the conformance suite every perago queue driver passes.
package drivertest

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/LouisRiverstone/perago"
)

// Factory returns a fresh, empty driver and a function moving its clock forward.
// Drivers that cannot fake time return a nil advance; the time-based cases then sleep.
type Factory func(t *testing.T) (d perago.Driver, advance func(time.Duration))

func msg(queue, body string) perago.Message {
	return perago.Message{ID: perago.NewUUID(), Queue: queue, Payload: []byte(body)}
}

func pass(advance func(time.Duration), d time.Duration) {
	if advance != nil {
		advance(d)
		return
	}
	time.Sleep(d)
}

// Run runs the suite. retryAfter is the driver's configured reservation timeout.
func Run(t *testing.T, newDriver Factory, retryAfter time.Duration) {
	ctx := context.Background()

	t.Run("push pop delete", func(t *testing.T) {
		d, _ := newDriver(t)
		if _, err := d.Push(ctx, msg("q1", `{"n":1}`)); err != nil {
			t.Fatal(err)
		}
		r, err := d.Pop(ctx, "q1")
		if err != nil || r == nil {
			t.Fatalf("pop: %v %v", r, err)
		}
		if string(r.Payload()) != `{"n":1}` || r.Attempts() != 1 || r.Queue() != "q1" || r.ID() == "" {
			t.Fatalf("reserved: %s attempts=%d queue=%s id=%s", r.Payload(), r.Attempts(), r.Queue(), r.ID())
		}
		if again, _ := d.Pop(ctx, "q1"); again != nil {
			t.Fatal("a reserved job was popped twice")
		}
		if err := r.Delete(ctx); err != nil {
			t.Fatal(err)
		}
		if n, _ := d.Size(ctx, "q1"); n != 0 {
			t.Fatalf("size after delete = %d", n)
		}
	})

	t.Run("fifo and queues are isolated", func(t *testing.T) {
		d, _ := newDriver(t)
		for i := range 3 {
			if _, err := d.Push(ctx, msg("a", strconv.Itoa(i))); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := d.Push(ctx, msg("b", "other")); err != nil {
			t.Fatal(err)
		}
		for i := range 3 {
			r, err := d.Pop(ctx, "a")
			if err != nil || r == nil {
				t.Fatalf("pop %d: %v", i, err)
			}
			if string(r.Payload()) != strconv.Itoa(i) {
				t.Fatalf("order: got %s want %d", r.Payload(), i)
			}
			_ = r.Delete(ctx)
		}
		if r, _ := d.Pop(ctx, "a"); r != nil {
			t.Fatal("queue a should be empty")
		}
		if n, _ := d.Size(ctx, "b"); n != 1 {
			t.Fatalf("size b = %d", n)
		}
	})

	t.Run("delay", func(t *testing.T) {
		d, advance := newDriver(t)
		m := msg("delayed", "x")
		m.Delay = 2 * time.Second
		if _, err := d.Push(ctx, m); err != nil {
			t.Fatal(err)
		}
		if r, _ := d.Pop(ctx, "delayed"); r != nil {
			t.Fatal("delayed job popped early")
		}
		if n, _ := d.Size(ctx, "delayed"); n != 1 {
			t.Fatalf("size counts delayed jobs: %d", n)
		}
		pass(advance, 3*time.Second)
		r, err := d.Pop(ctx, "delayed")
		if err != nil || r == nil {
			t.Fatalf("delayed job not available: %v", err)
		}
		_ = r.Delete(ctx)
	})

	t.Run("release increments attempts", func(t *testing.T) {
		d, advance := newDriver(t)
		if _, err := d.Push(ctx, msg("rel", "x")); err != nil {
			t.Fatal(err)
		}
		r, _ := d.Pop(ctx, "rel")
		if err := r.Release(ctx, 0); err != nil {
			t.Fatal(err)
		}
		r2, err := d.Pop(ctx, "rel")
		if err != nil || r2 == nil {
			t.Fatalf("released job not available: %v", err)
		}
		if r2.Attempts() != 2 {
			t.Fatalf("attempts = %d, want 2", r2.Attempts())
		}
		if err := r2.Release(ctx, 2*time.Second); err != nil {
			t.Fatal(err)
		}
		if r3, _ := d.Pop(ctx, "rel"); r3 != nil {
			t.Fatal("job released with a delay came back early")
		}
		pass(advance, 3*time.Second)
		r3, _ := d.Pop(ctx, "rel")
		if r3 == nil || r3.Attempts() != 3 {
			t.Fatalf("after delayed release: %v", r3)
		}
		_ = r3.Delete(ctx)
	})

	t.Run("retry after expires reservations", func(t *testing.T) {
		if retryAfter <= 0 {
			t.Skip("driver has no retry_after")
		}
		d, advance := newDriver(t)
		if advance == nil && retryAfter > 3*time.Second {
			t.Skip("retry_after too long to wait for")
		}
		if _, err := d.Push(ctx, msg("exp", "x")); err != nil {
			t.Fatal(err)
		}
		if r, _ := d.Pop(ctx, "exp"); r == nil {
			t.Fatal("pop")
		}
		pass(advance, retryAfter+time.Second)
		r, err := d.Pop(ctx, "exp")
		if err != nil || r == nil {
			t.Fatalf("expired reservation not available again: %v", err)
		}
		if r.Attempts() != 2 {
			t.Fatalf("attempts = %d", r.Attempts())
		}
		_ = r.Delete(ctx)
	})

	t.Run("clear", func(t *testing.T) {
		d, _ := newDriver(t)
		for range 3 {
			if _, err := d.Push(ctx, msg("clr", "x")); err != nil {
				t.Fatal(err)
			}
		}
		n, err := d.Clear(ctx, "clr")
		if err != nil || n != 3 {
			t.Fatalf("clear = %d, %v", n, err)
		}
		if s, _ := d.Size(ctx, "clr"); s != 0 {
			t.Fatalf("size after clear = %d", s)
		}
	})

	t.Run("bulk", func(t *testing.T) {
		d, _ := newDriver(t)
		bp, ok := d.(perago.BulkPusher)
		if !ok {
			t.Skip("no bulk push")
		}
		ids, err := bp.PushBulk(ctx, []perago.Message{msg("bulk", "1"), msg("bulk", "2")})
		if err != nil || len(ids) != 2 {
			t.Fatalf("bulk: %v %v", ids, err)
		}
		if n, _ := d.Size(ctx, "bulk"); n != 2 {
			t.Fatalf("size = %d", n)
		}
	})

	t.Run("size breakdown", func(t *testing.T) {
		d, _ := newDriver(t)
		sr, ok := d.(perago.SizeReporter)
		if !ok {
			t.Skip("no size breakdown")
		}
		_, _ = d.Push(ctx, msg("sz", "ready"))
		_, _ = d.Push(ctx, msg("sz", "ready2"))
		late := msg("sz", "late")
		late.Delay = time.Hour
		_, _ = d.Push(ctx, late)
		if r, _ := d.Pop(ctx, "sz"); r == nil {
			t.Fatal("pop")
		}
		p, _ := sr.PendingSize(ctx, "sz")
		dl, _ := sr.DelayedSize(ctx, "sz")
		rs, _ := sr.ReservedSize(ctx, "sz")
		if p != 1 || dl != 1 || rs != 1 {
			t.Fatalf("pending=%d delayed=%d reserved=%d", p, dl, rs)
		}
		if ar, ok := d.(perago.AgeReporter); ok {
			if ts, err := ar.OldestPending(ctx, "sz"); err != nil || ts.IsZero() {
				t.Fatalf("oldest pending: %v %v", ts, err)
			}
		}
	})
}

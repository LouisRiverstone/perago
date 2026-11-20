package redis_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/internal/cachetest"
	"github.com/LouisRiverstone/perago/internal/drivertest"
	"github.com/LouisRiverstone/perago/redis"
	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func client(t *testing.T) (*miniredis.Miniredis, goredis.UniversalClient) {
	t.Helper()
	srv := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return srv, c
}

func TestDriverConformance(t *testing.T) {
	drivertest.Run(t, func(t *testing.T) (perago.Driver, func(time.Duration)) {
		_, c := client(t)
		clk := &clock{t: time.Unix(1_800_000_000, 0)}
		return redis.NewDriver(c, redis.DriverOptions{RetryAfter: 5 * time.Second, Now: clk.Now}), clk.Advance
	}, 5*time.Second)
}

func TestBlockFor(t *testing.T) {
	_, c := client(t)
	d := redis.NewDriver(c, redis.DriverOptions{BlockFor: 50 * time.Millisecond})
	start := time.Now()
	r, err := d.Pop(context.Background(), "empty")
	if err != nil || r != nil {
		t.Fatalf("pop empty: %v %v", r, err)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatal("BlockFor did not block")
	}
	go func() {
		time.Sleep(10 * time.Millisecond)
		_, _ = d.Push(context.Background(), perago.Message{ID: perago.NewUUID(), Queue: "empty", Payload: []byte("x")})
	}()
	d2 := redis.NewDriver(c, redis.DriverOptions{BlockFor: time.Second})
	time.Sleep(20 * time.Millisecond)
	r, err = d2.Pop(context.Background(), "empty")
	if err != nil || r == nil || r.Attempts() != 1 {
		t.Fatalf("pop after push: %v %v", r, err)
	}
}

func TestCacheConformance(t *testing.T) {
	cachetest.Run(t, func(t *testing.T) (perago.Cache, func(time.Duration)) {
		srv, c := client(t)
		return redis.NewCache(c, ""), srv.FastForward
	})
}

func TestRejectsNonUUIDIDs(t *testing.T) {
	_, c := client(t)
	d := redis.NewDriver(c, redis.DriverOptions{})
	if _, err := d.Push(context.Background(), perago.Message{ID: "short", Queue: "q"}); err == nil {
		t.Fatal("short id accepted")
	}
}

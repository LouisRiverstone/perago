package perago_test

import (
	"sync"
	"testing"
	"time"

	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/internal/cachetest"
	"github.com/LouisRiverstone/perago/internal/drivertest"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Unix(1_800_000_000, 0)} }

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

func TestMemoryDriverConformance(t *testing.T) {
	drivertest.Run(t, func(t *testing.T) (perago.Driver, func(time.Duration)) {
		c := newClock()
		d := perago.NewMemoryDriver(5 * time.Second)
		d.SetClock(c.Now)
		return d, c.Advance
	}, 5*time.Second)
}

func TestMemoryCacheConformance(t *testing.T) {
	cachetest.Run(t, func(t *testing.T) (perago.Cache, func(time.Duration)) {
		c := newClock()
		mc := perago.NewMemoryCache()
		mc.SetClock(c.Now)
		return mc, c.Advance
	})
}

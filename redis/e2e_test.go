package redis_test

import (
	"testing"
	"time"

	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/internal/e2etest"
	"github.com/LouisRiverstone/perago/redis"
)

// The queue and the cache (locks, rate limits, worker signals) on Redis; failed jobs and
// batches stay in memory, as Redis has no store for them.
func TestEndToEndRedis(t *testing.T) {
	e2etest.Run(t, func(t *testing.T, now func() time.Time) e2etest.Backend {
		srv, c := client(t)
		return e2etest.Backend{
			Driver:  redis.NewDriver(c, redis.DriverOptions{RetryAfter: 90 * time.Second, Now: now}),
			Cache:   redis.NewCache(c, ""),
			Failed:  perago.NewMemoryFailedJobStore(),
			Batches: perago.NewMemoryBatchStore(),
			Advance: srv.FastForward,
		}
	})
}

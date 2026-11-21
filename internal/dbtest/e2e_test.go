package dbtest_test

import (
	"testing"
	"time"

	"github.com/LouisRiverstone/perago/database"
	"github.com/LouisRiverstone/perago/internal/e2etest"
)

// Every scenario with the queue, the cache, failed jobs and batches all in the database.
func TestEndToEndDatabase(t *testing.T) {
	e2etest.Run(t, func(t *testing.T, now func() time.Time) e2etest.Backend {
		db := setup(t)
		cache := database.NewCache(db, "")
		cache.SetClock(now)
		return e2etest.Backend{
			Driver:  database.NewDriver(db, database.DriverOptions{RetryAfter: 90 * time.Second, Now: now}),
			Cache:   cache,
			Failed:  database.NewFailedJobStore(db, ""),
			Batches: database.NewBatchStore(db, ""),
		}
	})
}

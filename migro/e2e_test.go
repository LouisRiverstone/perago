package migro_test

import (
	"testing"
	"time"

	"github.com/LouisRiverstone/perago/database"
	"github.com/LouisRiverstone/perago/internal/e2etest"
)

// Every scenario on tables created by the migro migrations, through a migro connection.
func TestEndToEndMigro(t *testing.T) {
	e2etest.Run(t, func(t *testing.T, now func() time.Time) e2etest.Backend {
		_, db := setup(t)
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

package perago_test

import (
	"testing"
	"time"

	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/internal/e2etest"
)

func TestEndToEndMemory(t *testing.T) {
	e2etest.Run(t, func(t *testing.T, now func() time.Time) e2etest.Backend {
		q := perago.NewMemoryDriver(90 * time.Second)
		q.SetClock(now)
		return e2etest.Backend{Driver: q, Failed: perago.NewMemoryFailedJobStore(), Batches: perago.NewMemoryBatchStore()}
	})
}

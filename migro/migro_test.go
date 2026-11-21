package migro_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	migrodb "github.com/LouisRiverstone/migro"
	"github.com/LouisRiverstone/migro/schema"
	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/database"
	"github.com/LouisRiverstone/perago/internal/cachetest"
	"github.com/LouisRiverstone/perago/internal/drivertest"
	peragomigro "github.com/LouisRiverstone/perago/migro"
	"github.com/LouisRiverstone/perago/migro/internal/testdb"
)

var ctx = context.Background()

// setup creates the tables with the migro migrations, then hands the connection to perago.
func setup(t *testing.T) (*migrodb.DB, *database.DB) {
	t.Helper()
	db := testdb.Open(t, database.JobsTable, database.FailedTable, database.BatchesTable, database.CacheTable)
	s := schema.New(db)
	for _, create := range []func(context.Context, *schema.Builder, string) error{
		peragomigro.CreateJobsTable, peragomigro.CreateFailedJobsTable, peragomigro.CreateBatchesTable, peragomigro.CreateCacheTable,
	} {
		if err := create(ctx, s, ""); err != nil {
			t.Fatal(err)
		}
	}
	qdb, err := peragomigro.Connect(db)
	if err != nil {
		t.Fatal(err)
	}
	return db, qdb
}

type clock struct{ t atomic.Int64 }

func (c *clock) Now() time.Time          { return time.Unix(c.t.Load(), 0) }
func (c *clock) Advance(d time.Duration) { c.t.Add(int64(d / time.Second)) }

func TestDriverOnMigroTables(t *testing.T) {
	drivertest.Run(t, func(t *testing.T) (perago.Driver, func(time.Duration)) {
		_, qdb := setup(t)
		c := &clock{}
		c.t.Store(1_800_000_000)
		return database.NewDriver(qdb, database.DriverOptions{RetryAfter: 5 * time.Second, Now: c.Now}), c.Advance
	}, 5*time.Second)
}

func TestCacheOnMigroTables(t *testing.T) {
	cachetest.Run(t, func(t *testing.T) (perago.Cache, func(time.Duration)) {
		_, qdb := setup(t)
		c := &clock{}
		c.t.Store(1_800_000_000)
		cache := database.NewCache(qdb, "")
		cache.SetClock(c.Now)
		return cache, c.Advance
	})
}

type Charge struct {
	perago.Queueable
	Amount int
}

var charged atomic.Int64

func (c *Charge) Handle(context.Context) error { charged.Add(int64(c.Amount)); return nil }

func init() { perago.Register(&Charge{}) }

func TestTransactionAfterCommit(t *testing.T) {
	db, qdb := setup(t)
	m, err := perago.New(perago.Config{
		Default:     "database",
		Connections: map[string]perago.Connection{"database": {Driver: database.NewDriver(qdb, database.DriverOptions{}), AfterCommit: true}},
		Failed:      database.NewFailedJobStore(qdb, ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = peragomigro.Transaction(ctx, m, db, func(ctx context.Context, tx *migrodb.DB) error {
		_, err := m.Dispatch(&Charge{Amount: 5}).Send(ctx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = peragomigro.Transaction(ctx, m, db, func(ctx context.Context, tx *migrodb.DB) error {
		_, _ = m.Dispatch(&Charge{Amount: 1000}).Send(ctx)
		return perago.Newf(perago.InvalidConfig, "rollback")
	})
	charged.Store(0)
	if _, err := m.Work(ctx, perago.WorkerOptions{StopWhenEmpty: true, Sleep: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if charged.Load() != 5 {
		t.Fatalf("charged = %d", charged.Load())
	}
}

func TestConnectInsideMigroTransaction(t *testing.T) {
	db, _ := setup(t)
	err := db.Transaction(ctx, func(tx *migrodb.DB) error {
		qtx, err := peragomigro.Connect(tx)
		if err != nil {
			return err
		}
		_, err = database.NewDriver(qtx, database.DriverOptions{}).Push(ctx, perago.Message{ID: perago.NewUUID(), Queue: "tx", Payload: []byte("{}")})
		if err != nil {
			return err
		}
		return perago.Newf(perago.InvalidConfig, "rollback")
	})
	if err == nil {
		t.Fatal("expected rollback error")
	}
	qdb, _ := peragomigro.Connect(db)
	if n, _ := database.NewDriver(qdb, database.DriverOptions{}).Size(ctx, "tx"); n != 0 {
		t.Fatalf("push survived the rollback: %d", n)
	}
	if _, err := peragomigro.Connect(nil); !perago.IsCode(err, perago.InvalidConfig) {
		t.Fatalf("nil = %v", err)
	}
}

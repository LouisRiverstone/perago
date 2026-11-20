package database_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LouisRiverstone/migro"
	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/database"
	"github.com/LouisRiverstone/perago/internal/cachetest"
	"github.com/LouisRiverstone/perago/internal/drivertest"
	"github.com/LouisRiverstone/perago/internal/testdb"
)

var ctx = context.Background()

var allTables = []string{database.JobsTable, database.FailedTable, database.BatchesTable, database.CacheTable}

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

func setup(t *testing.T) *migro.DB {
	t.Helper()
	db := testdb.Open(t, allTables...)
	if err := database.Install(ctx, db, database.Tables{}); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestDriverConformance(t *testing.T) {
	drivertest.Run(t, func(t *testing.T) (perago.Driver, func(time.Duration)) {
		db := setup(t)
		c := &clock{t: time.Unix(1_800_000_000, 0)}
		return database.NewDriver(db, database.DriverOptions{RetryAfter: 5 * time.Second, Now: c.Now}), c.Advance
	}, 5*time.Second)
}

func TestCacheConformance(t *testing.T) {
	cachetest.Run(t, func(t *testing.T) (perago.Cache, func(time.Duration)) {
		db := setup(t)
		c := &clock{t: time.Unix(1_800_000_000, 0)}
		cache := database.NewCache(db, "")
		cache.SetClock(c.Now)
		return cache, c.Advance
	})
}

func TestConcurrentPopNeverDuplicates(t *testing.T) {
	db := setup(t)
	d := database.NewDriver(db, database.DriverOptions{})
	const jobs = 40
	for i := range jobs {
		if _, err := d.Push(ctx, perago.Message{ID: perago.NewUUID(), Queue: "c", Payload: []byte{byte('a' + i%26)}}); err != nil {
			t.Fatal(err)
		}
	}
	var seen sync.Map
	var got atomic.Int32
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				r, err := d.Pop(ctx, "c")
				if err != nil {
					t.Error(err)
					return
				}
				if r == nil {
					return
				}
				if _, dup := seen.LoadOrStore(r.ID(), true); dup {
					t.Errorf("job %s popped twice", r.ID())
				}
				got.Add(1)
				_ = r.Delete(ctx)
			}
		}()
	}
	wg.Wait()
	if got.Load() != jobs {
		t.Fatalf("popped %d of %d", got.Load(), jobs)
	}
}

func TestLargePayload(t *testing.T) {
	db := setup(t)
	d := database.NewDriver(db, database.DriverOptions{})
	big := strings.Repeat("x", 200_000)
	if _, err := d.Push(ctx, perago.Message{ID: perago.NewUUID(), Queue: "big", Payload: []byte(big)}); err != nil {
		t.Fatal(err)
	}
	r, err := d.Pop(ctx, "big")
	if err != nil || r == nil || len(r.Payload()) != len(big) {
		t.Fatalf("large payload: %v %v", r, err)
	}
}

func TestFailedJobStore(t *testing.T) {
	db := setup(t)
	s := database.NewFailedJobStore(db, "")
	now := time.Unix(1_800_000_000, 0)
	for i, q := range []string{"a", "b", "a"} {
		f := perago.FailedJob{ID: perago.NewUUID(), Connection: "database", Queue: q, Payload: []byte(`{"job":"x"}`), Exception: "boom", FailedAt: now.Add(time.Duration(i) * time.Hour)}
		if err := s.Log(ctx, f); err != nil {
			t.Fatal(err)
		}
	}
	all, err := s.All(ctx)
	if err != nil || len(all) != 3 || !all[0].FailedAt.After(all[2].FailedAt) {
		t.Fatalf("all = %+v %v", all, err)
	}
	ids, _ := s.IDs(ctx, "a")
	if len(ids) != 2 {
		t.Fatalf("ids(a) = %v", ids)
	}
	f, err := s.Find(ctx, ids[0])
	if err != nil || f.Queue != "a" || string(f.Payload) != `{"job":"x"}` {
		t.Fatalf("find = %+v %v", f, err)
	}
	if _, err := s.Find(ctx, "missing"); !perago.IsCode(err, perago.FailedJobNotFound) {
		t.Fatalf("missing = %v", err)
	}
	if ok, _ := s.Forget(ctx, ids[0]); !ok {
		t.Fatal("forget")
	}
	if n, _ := s.Prune(ctx, now.Add(90*time.Minute)); n != 1 {
		t.Fatalf("prune = %d", n)
	}
	if n, _ := s.Flush(ctx, 0); n != 1 {
		t.Fatalf("flush = %d", n)
	}
}

func TestBatchStore(t *testing.T) {
	db := setup(t)
	s := database.NewBatchStore(db, "")
	created := time.Unix(1_800_000_000, 0)
	b := &perago.Batch{ID: perago.NewUUID(), Name: "import", CreatedAt: created, Options: perago.BatchOptions{AllowFailures: true, Queue: "q"}}
	if err := s.Store(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := s.IncrementTotalJobs(ctx, b.ID, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordFailure(ctx, b.ID, "j1"); err != nil {
		t.Fatal(err)
	}
	got, err := s.RecordSuccess(ctx, b.ID, "j1")
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalJobs != 3 || got.PendingJobs != 2 || got.FailedJobs != 1 || len(got.FailedJobIDs) != 0 || !got.Options.AllowFailures || got.Options.Queue != "q" {
		t.Fatalf("batch = %+v", got)
	}
	if err := s.Cancel(ctx, b.ID, created.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	f, _ := s.Find(ctx, b.ID)
	if f.CancelledAt == nil || f.FinishedAt == nil {
		t.Fatalf("cancelled = %+v", f)
	}
	b2 := &perago.Batch{ID: perago.NewUUID(), Name: "later", CreatedAt: created.Add(time.Minute)}
	_ = s.Store(ctx, b2)
	list, _ := s.List(ctx, 10, "")
	if len(list) != 2 || list[0].ID != b2.ID {
		t.Fatalf("list = %v", list)
	}
	older, _ := s.List(ctx, 10, b2.ID)
	if len(older) != 1 || older[0].ID != b.ID {
		t.Fatalf("list before = %v", older)
	}
	if n, _ := s.PruneCancelled(ctx, created.Add(2*time.Hour)); n != 1 {
		t.Fatalf("prune cancelled = %d", n)
	}
	if n, _ := s.PruneUnfinished(ctx, created.Add(2*time.Hour)); n != 1 {
		t.Fatalf("prune unfinished = %d", n)
	}
	if _, err := s.Find(ctx, b.ID); !perago.IsCode(err, perago.BatchNotFound) {
		t.Fatalf("pruned batch found: %v", err)
	}
}

type Charge struct {
	perago.Queueable
	Amount int
}

var charged atomic.Int64

func (c *Charge) Handle(context.Context) error { charged.Add(int64(c.Amount)); return nil }

func init() { perago.Register(&Charge{}) }

func TestEndToEndWithAfterCommit(t *testing.T) {
	db := setup(t)
	m, err := perago.New(perago.Config{
		Default:     "database",
		Connections: map[string]perago.Connection{"database": {Driver: database.NewDriver(db, database.DriverOptions{}), AfterCommit: true}},
		Failed:      database.NewFailedJobStore(db, ""),
		Batches:     database.NewBatchStore(db, ""),
		Cache:       database.NewCache(db, ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = database.Transaction(ctx, m, db, func(ctx context.Context, tx *migro.DB) error {
		_, err := m.Dispatch(&Charge{Amount: 5}).Send(ctx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = database.Transaction(ctx, m, db, func(ctx context.Context, tx *migro.DB) error {
		_, _ = m.Dispatch(&Charge{Amount: 1000}).Send(ctx)
		return perago.Newf(perago.InvalidConfig, "rollback")
	})
	var done atomic.Int32
	_, err = m.Batch(&Charge{Amount: 1}, &Charge{Amount: 2}).Then(func(ctx context.Context, b *perago.Batch) { done.Add(1) }).Dispatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	charged.Store(0)
	if _, err := m.Work(ctx, perago.WorkerOptions{StopWhenEmpty: true, Sleep: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if charged.Load() != 8 || done.Load() != 1 {
		t.Fatalf("charged=%d then=%d", charged.Load(), done.Load())
	}
}

func TestCacheLongKeys(t *testing.T) {
	db := setup(t)
	c := database.NewCache(db, "")
	long := strings.Repeat("k", 600)
	if ok, err := c.Add(ctx, long, "v", time.Minute); !ok || err != nil {
		t.Fatalf("add long key: %v %v", ok, err)
	}
	if v, ok, _ := c.Get(ctx, long); !ok || v != "v" {
		t.Fatal("long key not found")
	}
	if ok, _ := c.Add(ctx, long, "w", time.Minute); ok {
		t.Fatal("long key added twice")
	}
}

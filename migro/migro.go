// Package migro connects perago to migro (github.com/LouisRiverstone/migro). perago itself never
// needs migro; this module is for applications that already use it:
//
//	import peragomigro "github.com/LouisRiverstone/perago/migro"
//
//	db, _ := migro.Open(migro.Config{Dialect: "postgres", DSN: dsn})
//	qdb, _ := peragomigro.Connect(db)
//	driver := database.NewDriver(qdb, database.DriverOptions{})
//
// It also holds the migro migrations for perago's tables (make:queue-table writes them on a
// migro project) and, in the model subpackage, Laravel's SerializesModels for migro models.
package migro

import (
	"context"

	migrodb "github.com/LouisRiverstone/migro"
	"github.com/LouisRiverstone/migro/schema"
	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/database"
)

// Connect wraps a migro connection, or a transaction handed out by db.Transaction, for perago's
// database driver and stores.
func Connect(db *migrodb.DB) (*database.DB, error) {
	if db == nil || db.Gorm == nil {
		return nil, perago.Newf(perago.InvalidConfig, "migro: nil database")
	}
	return database.New(db.Gorm.Statement.ConnPool, db.Dialect.Name())
}

// Transaction runs fn in a migro transaction and an after-commit scope of m: jobs dispatched with
// the context fn receives to an AfterCommit connection are pushed only once the transaction
// commits, and dropped when it rolls back.
func Transaction(ctx context.Context, m *perago.Manager, db *migrodb.DB, fn func(ctx context.Context, tx *migrodb.DB) error) error {
	return m.Transaction(ctx, func(ctx context.Context) error {
		return db.Transaction(ctx, func(tx *migrodb.DB) error { return fn(ctx, tx) })
	})
}

func or(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// CreateJobsTable creates the queue table (make:queue-table).
func CreateJobsTable(ctx context.Context, s *schema.Builder, name string) error {
	name = or(name, database.JobsTable)
	err := s.CreateTable(ctx, name, func(t *schema.Table) {
		t.BigIncrements("id")
		t.String("queue").NotNull().Index()
		t.Text("payload").NotNull()
		t.Integer("attempts").NotNull().Default(0)
		t.BigInteger("reserved_at").Nullable()
		t.BigInteger("available_at").NotNull()
		t.BigInteger("created_at").NotNull()
		t.Index("queue", "reserved_at", "available_at")
	})
	if err != nil {
		return err
	}
	return widenText(ctx, s, name, "payload")
}

// CreateFailedJobsTable creates the failed jobs table (make:queue-failed-table).
func CreateFailedJobsTable(ctx context.Context, s *schema.Builder, name string) error {
	name = or(name, database.FailedTable)
	err := s.CreateTable(ctx, name, func(t *schema.Table) {
		t.String("uuid", 36).Primary()
		t.String("connection").NotNull()
		t.String("queue").NotNull().Index()
		t.Text("payload").NotNull()
		t.Text("exception").NotNull()
		t.BigInteger("failed_at").NotNull().Index()
	})
	if err != nil {
		return err
	}
	return widenText(ctx, s, name, "payload", "exception")
}

// CreateBatchesTable creates the job batches table (make:queue-batches-table).
func CreateBatchesTable(ctx context.Context, s *schema.Builder, name string) error {
	name = or(name, database.BatchesTable)
	err := s.CreateTable(ctx, name, func(t *schema.Table) {
		t.String("id", 36).Primary()
		t.String("name").NotNull()
		t.Integer("total_jobs").NotNull()
		t.Integer("pending_jobs").NotNull()
		t.Integer("failed_jobs").NotNull()
		t.Text("failed_job_ids").NotNull()
		t.Text("options").Nullable()
		t.BigInteger("cancelled_at").Nullable()
		t.BigInteger("created_at").NotNull().Index()
		t.BigInteger("finished_at").Nullable()
	})
	if err != nil {
		return err
	}
	return widenText(ctx, s, name, "failed_job_ids", "options")
}

// CreateCacheTable creates the cache table used for locks, rate limits and worker signals
// (make:cache-table).
func CreateCacheTable(ctx context.Context, s *schema.Builder, name string) error {
	return s.CreateTable(ctx, or(name, database.CacheTable), func(t *schema.Table) {
		t.String("key").Primary()
		t.String("value", database.CacheValueLength).NotNull()
		t.BigInteger("expiration").NotNull().Index()
	})
}

// widenText turns TEXT columns into LONGTEXT on MySQL and MariaDB, where TEXT stops at 64 KB.
func widenText(ctx context.Context, s *schema.Builder, table string, cols ...string) error {
	d := s.DB().Dialect
	if d.Name() != "mysql" && d.Name() != "mariadb" {
		return nil
	}
	for _, c := range cols {
		null := " NOT NULL"
		if c == "options" {
			null = " NULL"
		}
		if err := s.Raw(ctx, "ALTER TABLE "+d.Quote(table)+" MODIFY "+d.Quote(c)+" LONGTEXT"+null); err != nil {
			return err
		}
	}
	return nil
}

---
title: Using with migro
nav_order: 4
description: "Optional migro integration: connection, transactions, migrations and models. perago never requires migro."
---

# Using with migro

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

[migro](https://github.com/LouisRiverstone/migro) is the Laravel-style database toolkit for Go that perago was built next to. The two are **compatible, and independent**:

- perago **never requires** migro. The core module, the `database` driver and stores, and every other module run on plain `database/sql`. A project without migro downloads nothing of migro or GORM.
- On a project that **already uses** migro, the optional `perago/migro` module plugs perago into it: the same connection and transactions, perago's tables as migro migrations, and migro models in job payloads.

| | Without migro | With migro |
|---|---|---|
| Connection | `database.New(sqlDB, "postgres")` | `peragomigro.Connect(mdb)` |
| After-commit dispatch | `database.Transaction(ctx, m, sqlDB, fn)` | `peragomigro.Transaction(ctx, m, mdb, fn)` |
| Creating the tables | `.sql` files from `make:*-table`, or `database.Install` | migro migrations from `make:*-table`, run by `migro migration:run` |
| Models in jobs | pass the key, load the row yourself | `model.Of[T]` / `model.Many[T]` |
| Modules to `go get` | `perago` | `perago` and `perago/migro` |

## Install

```
go get github.com/LouisRiverstone/perago@latest
go get github.com/LouisRiverstone/perago/migro@latest
```

The package is named `migro`, like migro's own; import it under another name:

```go
import (
	"github.com/LouisRiverstone/migro"
	_ "github.com/LouisRiverstone/migro/dialect/postgres" // the dialects you use
	peragomigro "github.com/LouisRiverstone/perago/migro"
)
```

## Connecting

`Connect` hands a `*migro.DB` to perago's `database` package. The driver and the stores then run on migro's connection pool, in migro's dialect:

```go
mdb, err := migro.Open(migro.Config{Dialect: "postgres", DSN: os.Getenv("DATABASE_URL")})
db, err := peragomigro.Connect(mdb)

m, err := perago.New(perago.Config{
	Default: "database",
	Connections: map[string]perago.Connection{
		"database": {Driver: database.NewDriver(db, database.DriverOptions{}), AfterCommit: true},
	},
	Failed:  database.NewFailedJobStore(db, ""),
	Batches: database.NewBatchStore(db, ""),
	Cache:   database.NewCache(db, ""),
})
```

Every migro dialect works: PostgreSQL, CockroachDB, Redshift, MySQL, MariaDB, SQLite, SQL Server and Oracle.

`Connect` also accepts a transaction handed out by `mdb.Transaction`. A job pushed through it is written in that transaction and disappears if it rolls back:

```go
err := mdb.Transaction(ctx, func(tx *migro.DB) error {
	qtx, err := peragomigro.Connect(tx)
	if err != nil {
		return err
	}
	_, err = database.NewDriver(qtx, database.DriverOptions{}).Push(ctx, msg)
	return err
})
```

Most of the time you want the next section instead: dispatch as usual and let perago wait for the commit.

## Dispatching after commit

`Transaction` runs your function in a migro transaction and in perago's after-commit scope. Jobs dispatched with the context it hands you to an `AfterCommit` connection are pushed once the transaction commits, and dropped when it rolls back (Laravel's `after_commit`):

```go
err := peragomigro.Transaction(ctx, m, mdb, func(ctx context.Context, tx *migro.DB) error {
	order, err := orders.CreateWith(ctx, tx, input)
	if err != nil {
		return err // rolled back: SendReceipt is never pushed
	}
	_, err = m.Dispatch(&SendReceipt{OrderID: order.ID}).Send(ctx)
	return err
})
```

See [Dispatching](dispatching.md#jobs-and-database-transactions) for the per-job and per-dispatch switches.

## Migrations

On a project whose `go.mod` requires migro, the table generators write migro migrations instead of `.sql` files:

```
perago make:queue-table
perago make:queue-failed-table
perago make:queue-batches-table
perago make:cache-table
migro migration:run
```

`--table=` renames a table, and `--format=sql` writes a `.sql` file even on a migro project. A generated migration calls the module's table builders:

```go
type CreateJobsTable struct{}

func init() { migrate.Register(&CreateJobsTable{}) }

func (CreateJobsTable) Up(ctx context.Context, s *schema.Builder) error {
	return peragomigro.CreateJobsTable(ctx, s, "jobs")
}

func (CreateJobsTable) Down(ctx context.Context, s *schema.Builder) error {
	return s.DropTableIfExists(ctx, "jobs")
}
```

| Builder | Table |
|---|---|
| `CreateJobsTable(ctx, s, name)` | the queue (`jobs`) |
| `CreateFailedJobsTable(ctx, s, name)` | failed jobs (`failed_jobs`) |
| `CreateBatchesTable(ctx, s, name)` | job batches (`job_batches`) |
| `CreateCacheTable(ctx, s, name)` | the database cache for locks, rate limits and worker signals (`cache`) |

The tables are the same, column for column, as the ones `database.Install` and the `.sql` files create, including `LONGTEXT` payloads on MySQL and MariaDB. You can start with one and move to the other without touching data.

## Models in jobs

`model.Of[T]` (package `github.com/LouisRiverstone/perago/migro/model`) is Laravel's `SerializesModels` for migro models. Only the primary key goes into the payload; the worker loads the row again, fresh, right before `Handle`. Relations are never serialized, as with Laravel's `withoutRelations`; list the ones to load again with `With`.

```go
import "github.com/LouisRiverstone/perago/migro/model"

type SendInvoice struct {
	perago.Queueable
	perago.DeletesMissingModels          // #[DeleteWhenMissingModels]
	Order model.Of[Order]
	Lines model.Many[Line]
}

func init() { perago.Register(&SendInvoice{}) }

model.Use(mdb) // once, at startup: where models are loaded from

m.Dispatch(&SendInvoice{
	Order: model.New(order).With("Customer"),
	Lines: model.NewMany(lines),
}).Send(ctx)

func (j *SendInvoice) Handle(ctx context.Context) error {
	order := j.Order.Get() // *Order, loaded by the worker
	for _, line := range j.Lines.Get() {
		...
	}
	return nil
}
```

| API | Does |
|---|---|
| `model.New(&v)` / `model.NewMany(vs)` | references models by primary key |
| `model.Key[T](id)` | references a model by key without loading it first |
| `.With("Relation", ...)` | eager loads relations when the model is restored |
| `.On("name")` | loads from a database registered with `model.UseNamed("name", db)` |
| `.Get()` | the model: the one given before dispatch, the fresh copy inside the job |

When a row is gone, the job fails with `MODEL_NOT_FOUND`. With `perago.DeletesMissingModels` embedded (or `DeleteWhenMissingModels()` returning true), it is deleted quietly instead.

Without migro, keep the key in the job (`OrderID int64`) and load the row in `Handle`; any field type can also restore itself by implementing `perago.Restorable`.

## Testing against real databases

The module's tests create the tables with the migro migrations and run perago's full driver and cache suites on them, on SQLite by default and on every dialect in CI:

```
cd migro
PERAGO_TEST_DIALECT=postgres PERAGO_TEST_DSN="host=localhost user=postgres password=postgres dbname=perago sslmode=disable" \
  go test -p 1 -count=1 ./...
```

---
title: Getting started
nav_order: 2
description: "Install perago, create the tables, dispatch a job and run a worker."
---

# Getting started

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Install

Go 1.25 or newer.

```
go get github.com/LouisRiverstone/perago@latest
go install github.com/LouisRiverstone/perago/cmd/perago@latest   # optional: the CLI
```

perago is split into Go modules so a project downloads only what it uses. The core has no dependencies; each backend is a module you add when you need it:

| Module | `go get` | Brings in |
|---|---|---|
| core | `github.com/LouisRiverstone/perago` | nothing: the standard library only. Jobs, workers, chains, batches, middleware, `queuetest`, `console`, the `database` driver and stores (`database/sql`) |
| Redis | `github.com/LouisRiverstone/perago/redis` | `go-redis` |
| SQS | `github.com/LouisRiverstone/perago/sqs` | AWS SDK v2 (SQS) |
| DynamoDB | `github.com/LouisRiverstone/perago/dynamodb` | AWS SDK v2 (DynamoDB) |
| Beanstalkd | `github.com/LouisRiverstone/perago/beanstalkd` | `go-beanstalk` |
| migro | `github.com/LouisRiverstone/perago/migro` | [migro](https://github.com/LouisRiverstone/migro): `Connect`, migrations, `model.Of` |

{: .note }
**migro is optional.** perago is compatible with [migro](https://github.com/LouisRiverstone/migro) but never depends on it: the core, the database driver and every other module work without it, on any `database/sql` connection. Add the `perago/migro` module only if your project already uses migro, to reuse its connection and transactions, run perago's tables as migro migrations and serialize migro models with `model.Of`. Without it, nothing of migro (or GORM) is downloaded. See [Using with migro](migro.md).

## Connections and queues

As in Laravel, a **connection** is a backend (a database, a Redis server, an SQS account) and a **queue** is a named line of jobs on it. Every connection has a default queue, `default` unless you set `Connection.Queue`. A job goes to the default connection and queue unless it says otherwise:

```go
m.Dispatch(&ProcessPodcast{}).Send(ctx)                    // default connection, default queue
m.Dispatch(&ProcessPodcast{}).OnQueue("emails").Send(ctx)  // the "emails" queue
```

Workers take jobs from the queues you name, in priority order: `queue:work --queue=high,default`.

## Create the tables (database driver)

The database driver needs a `jobs` table, failed jobs need `failed_jobs`, batches need `job_batches`, and the database cache needs `cache`. Generate a migration for each:

```
perago make:queue-table --dialect=postgres
perago make:queue-failed-table --dialect=postgres
perago make:queue-batches-table --dialect=postgres
perago make:cache-table --dialect=postgres
```

They are `.sql` files for your migration tool. On a project whose `go.mod` requires migro they are migro migrations instead (run them with `migro migration:run`); `--format=sql|migro` picks explicitly.

Or create any missing tables at startup with `database.Install(ctx, db, database.Tables{})`.

## Build the manager

```go
import (
	"database/sql"

	_ "github.com/jackc/pgx/v5/stdlib" // any database/sql driver
	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/database"
)

sqlDB, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
db, err := database.New(sqlDB, "postgres")

m, err := perago.New(perago.Config{
	Default: "database",
	Connections: map[string]perago.Connection{
		"database": {Driver: database.NewDriver(db, database.DriverOptions{})},
		"sync":     {Driver: perago.SyncDriver()},
	},
	Failed:  database.NewFailedJobStore(db, ""),
	Batches: database.NewBatchStore(db, ""),
	Cache:   database.NewCache(db, ""),
	Key:     appKey, // 32 bytes, for encrypted jobs
})
```

When `Default` is empty, the `PERAGO_CONNECTION` or `QUEUE_CONNECTION` environment variable picks the connection, then `sync`.

## Write and dispatch a job

```
perago make:job ProcessPodcast
```

```go
type ProcessPodcast struct {
	perago.Queueable
	PodcastID int
}

func init() { perago.Register(&ProcessPodcast{}) }

func (j *ProcessPodcast) Handle(ctx context.Context) error {
	return transcode(ctx, j.PodcastID)
}
```

```go
id, err := m.Dispatch(&ProcessPodcast{PodcastID: 1}).Send(ctx)
```

## Run a worker

Workers live in your program, because they need your jobs and connections. Add a console command (see [Command line](command-line.md)):

```go
// cmd/perago/main.go
func main() {
	app := &console.App{Manager: func(ctx context.Context) (*perago.Manager, error) { return app.Queue(ctx) }}
	os.Exit(app.Run(context.Background(), os.Args[1:]))
}
```

```
go run ./cmd/perago queue:work
perago queue:work            # the perago CLI forwards to ./cmd/perago
```

Or run one inside your service, in a goroutine, with `m.Work(ctx, perago.WorkerOptions{Queues: []string{"high", "default"}})`: your application and the database are then all you need, no Redis and no separate worker process. See [Running the worker inside your application](workers.md#running-the-worker-inside-your-application) and [Choosing a backend](drivers.md#choosing-a-backend-database-or-redis).

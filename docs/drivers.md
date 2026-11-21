---
title: Connections and drivers
nav_order: 3
description: "Database (database/sql on eight dialects), Redis, SQS, Beanstalkd, sync, deferred, background, failover, null and memory."
---

# Connections and drivers

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Choosing a backend: database or Redis

perago needs four things: a **queue**, a **cache** (unique jobs, overlap locks, rate limits, `queue:restart` and `queue:pause`), a **failed job store** and a **batch store**. Any of them can live in your SQL database, so **Redis is never required**:

| | Database only | Redis for the queue | Nothing but the process |
|---|---|---|---|
| Queue | `database.NewDriver` | `redis.NewDriver` | `perago.NewMemoryDriver` |
| Cache | `database.NewCache` | `redis.NewCache` | `perago.NewMemoryCache` (the default) |
| Failed jobs | `database.NewFailedJobStore` | `database.NewFailedJobStore` | `perago.NewMemoryFailedJobStore` |
| Batches | `database.NewBatchStore` | `database.NewBatchStore` | `perago.NewMemoryBatchStore` |
| Survives a restart | yes | yes | no |
| Several processes or machines | yes | yes | no |
| Extra services to run | none | Redis | none |

The database is the simplest choice: your application and the database you already have, nothing else. Redis suits very high job rates, and blocking pops (`BlockFor`) pick jobs up without polling. You can also mix them, since every connection has its own driver: a `database` connection and a `redis` one side by side, each with its own workers.

### Database only, no Redis

```go
sqlDB, _ := sql.Open("pgx", os.Getenv("DATABASE_URL")) // or mysql, sqlite, sqlserver, oracle
db, _ := database.New(sqlDB, "postgres")
if err := database.Install(ctx, db, database.Tables{}); err != nil { // jobs, failed_jobs, job_batches, cache
	return err
}

m, err := perago.New(perago.Config{
	Default:     "database",
	Connections: map[string]perago.Connection{"database": {Driver: database.NewDriver(db, database.DriverOptions{})}},
	Failed:      database.NewFailedJobStore(db, ""),
	Batches:     database.NewBatchStore(db, ""),
	Cache:       database.NewCache(db, ""),
})
```

Run the worker inside the same process ([see Workers](workers.md#running-the-worker-inside-your-application)) or as `perago queue:work database` in another one. Any number of workers, on any number of machines, can share the table: the driver claims each job with a conditional `UPDATE`, so no two workers ever run the same one.

{: .warning }
Leaving `Config.Cache` empty gives an in-process `MemoryCache`. That is enough for a single process, but with two workers, or several instances of your application, set `database.NewCache` (or `redis.NewCache`): otherwise unique jobs, `WithoutOverlapping`, rate limits, `queue:restart` and `queue:pause` are not shared between them.

## Connections

A `perago.Connection` is one entry of Laravel's `config/queue.php`:

```go
perago.Connection{
	Driver:      database.NewDriver(db, database.DriverOptions{}),
	Queue:       "default", // the default queue
	AfterCommit: true,      // hold dispatches made inside a transaction until it commits
}
```

| Driver | Module | Constructor | Notes |
|---|---|---|---|
| database | core | `database.NewDriver(db, opts)` | Any `database/sql` connection, eight dialects. `retry_after` is `RetryAfter` (90s). |
| redis | `perago/redis` | `redis.NewDriver(client, opts)` | `go-redis` `UniversalClient`: single, Sentinel, Cluster. |
| sqs | `perago/sqs` | `sqs.NewDriver(client, opts)` | Standard, FIFO and fair queues; overflow storage. |
| beanstalkd | `perago/beanstalkd` | `beanstalkd.NewDriver(conn, opts)` | `retry_after` is the job TTR. |
| sync | core | `perago.SyncDriver()` | Runs the job now and returns its error. |
| deferred | core | `perago.DeferredDriver()` | Runs after the current request, in this process. |
| background | core | `perago.BackgroundDriver()` | Runs in its own goroutine. |
| failover | core | `perago.FailoverDriver("redis", "database")` | Pushes to the first connection that takes the job. |
| null | core | `perago.Null()` | Discards jobs. |
| memory | core | `perago.NewMemoryDriver(retryAfter)` | A full queue in this process, for tests and single-process apps. |

## Database

The `database` package is part of the core module and depends on `database/sql` alone. Open the connection with the driver you already use and name its dialect:

```go
sqlDB, _ := sql.Open("pgx", dsn) // pgx, go-sql-driver/mysql, modernc.org/sqlite, go-mssqldb, go-ora...
db, _ := database.New(sqlDB, "postgres")
d := database.NewDriver(db, database.DriverOptions{Table: "jobs", RetryAfter: 90 * time.Second})
```

| Dialect | Names `database.New` accepts | Placeholders |
|---|---|---|
| PostgreSQL | `postgres`, `postgresql`, `pg`, `pgx` | `$1` |
| CockroachDB | `cockroachdb`, `cockroach` | `$1` |
| Redshift | `redshift` | `$1` |
| MySQL / MariaDB | `mysql`, `mariadb` | `?` |
| SQLite | `sqlite`, `sqlite3` | `?` |
| SQL Server | `sqlserver`, `mssql` | `@p1` |
| Oracle | `oracle`, `oracledb` | `:1` |

`database.New` takes anything with `ExecContext`, `QueryContext` and `QueryRowContext`: a `*sql.DB`, a `*sql.Conn`, or an ORM's pool (GORM's `ConnPool`, for instance). Create the tables with `perago make:queue-table` (a `.sql` file, or a migro migration on a migro project), with `database.TableSQL`, or with `database.Install`. The driver runs on PostgreSQL, CockroachDB, Redshift, MySQL, MariaDB, SQLite, SQL Server and Oracle. To reserve a job, it selects a few available rows and then claims one with a conditional `UPDATE`. That update succeeds only while the row is still free and unchanged, so two workers never take the same job, and no dialect needs `SKIP LOCKED`.

On MySQL and MariaDB the payload columns are `LONGTEXT`, so payloads are not capped at 64 KB.

{: .note }
SQLite allows one writer at a time. Give the DSN a busy timeout (`?_pragma=busy_timeout(10000)`) when several workers share a file.

{: .warning }
Redshift does not enforce unique keys, so `database.NewCache` cannot provide atomic locks there. On Redshift, keep the queue in the database and use a Redis cache.

### With migro

On a migro project, `peragomigro.Connect(mdb)` turns a `*migro.DB` into the `db` above. migro is optional; see [Using with migro](migro.md).

## Redis

```
go get github.com/LouisRiverstone/perago/redis
```

```go
rdb := goredis.NewUniversalClient(&goredis.UniversalOptions{Addrs: []string{"localhost:6379"}})
d := redis.NewDriver(rdb, redis.DriverOptions{
	Prefix:     "myapp:",          // one per application
	RetryAfter: 90 * time.Second,
	BlockFor:   5 * time.Second,   // BLPOP instead of polling (block_for)
})
```

The driver takes a `go-redis` `UniversalClient`, so a single server, Sentinel and Cluster all work. A queue is a list of ready jobs plus sorted sets of delayed and reserved jobs. Lua scripts move jobs between them atomically, like Laravel's `RedisQueue`.

{: .warning }
Give each application its own `Prefix` (or Redis database) when several share a server, or they will take each other's jobs.

### Redis Cluster

On Redis Cluster, every key of a queue must land on the same slot, because the Lua scripts touch several of them at once. Wrap queue names in a hash tag:

```go
"redis": {Driver: redis.NewDriver(cluster, redis.DriverOptions{}), Queue: "{default}"},

m.Dispatch(job).OnQueue("{emails}").Send(ctx)
```

### Blocking

By default, a worker that finds its queue empty sleeps for `--sleep` and polls again. `BlockFor` makes it wait on Redis instead, with `BLPOP`, and take the job the moment it is pushed:

```go
redis.DriverOptions{BlockFor: 5 * time.Second}
```

Unlike Laravel's `block_for => 0`, a `BlockFor` of 0 does not block forever: it turns blocking off. Keep it a few seconds, so the worker still checks `queue:restart`, `queue:pause` and its signals regularly.

## Amazon SQS

```
go get github.com/LouisRiverstone/perago/sqs
```

```go
cfg, _ := config.LoadDefaultConfig(ctx)
d := sqs.NewDriver(awssqs.NewFromConfig(cfg), sqs.Options{
	Prefix:   "https://sqs.us-east-1.amazonaws.com/123456789012",
	Suffix:   "-production",        // optional, per environment
	WaitTime: 20 * time.Second,     // long polling
})
```

- The SQS visibility timeout plays the part of `retry_after`. Set it on the queue to be longer than the job timeout.
- **FIFO queues** (names ending in `.fifo`): `OnGroup("customer-1")` sets the message group, and the default group is the queue name. Jobs in a group run one at a time. The deduplication id is `WithDeduplicationID`, the job's `DeduplicationID()` method, or the job UUID. FIFO queues do not accept per-message delays.
- **Fair queues**: a message group on a standard queue enables fair delivery across groups.
- **Delays longer than 15 minutes** work: the job is sent again with the remaining delay until it is due.

### SQS overflow storage

SQS messages are limited to 1 MiB. With overflow storage, larger payloads are written to a store, and SQS carries only a pointer to them. The worker reads the payload back from the store:

```go
sqs.Options{Overflow: sqs.Overflow{
	Enabled:               true,
	Store:                 sqs.CacheOverflow{Cache: redis.NewCache(rdb, "sqs-overflow:")},
	Always:                false, // true stores every payload, not only large ones
	Threshold:             0,     // payload size that overflows; 0 is SQS's 1 MiB
	DeleteAfterProcessing: true,  // remove the stored payload once the job is deleted
	FlushOnClear:          false, // queue:clear also flushes the store
}}
```

Use a store that keeps payloads until the workers have processed them: one with no eviction, and a dedicated one if `FlushOnClear` is on, since flushing clears everything in it. Any `sqs.OverflowStore` works, for instance one backed by S3.

## Beanstalkd

```
go get github.com/LouisRiverstone/perago/beanstalkd
```

```go
conn, _ := beanstalkd.Dial("127.0.0.1:11300")
d := beanstalkd.NewDriver(conn, beanstalkd.Options{TTR: 90 * time.Second, BlockFor: 5 * time.Second})
```

## Other driver prerequisites

| Driver | Module | Brings in |
|---|---|---|
| database | core | nothing; you bring a `database/sql` driver (`pgx`, `go-sql-driver/mysql`, `modernc.org/sqlite`, `go-mssqldb`, `go-ora`...) |
| Redis | `perago/redis` | `github.com/redis/go-redis/v9` |
| Amazon SQS | `perago/sqs` | `github.com/aws/aws-sdk-go-v2/service/sqs` |
| Beanstalkd | `perago/beanstalkd` | `github.com/beanstalkd/go-beanstalk` |
| DynamoDB (failed jobs, batches) | `perago/dynamodb` | `github.com/aws/aws-sdk-go-v2/service/dynamodb` |

Laravel's MongoDB driver is a third-party package and has no perago counterpart.

## Sync, deferred and background

`sync` runs the job immediately in the calling goroutine. A failed job runs its `Failed` hook, and its error is returned by `Send`.

`deferred` runs the job in this process after the current request. Wrap your HTTP handler with `m.Middleware(handler)`, or open a scope yourself with `ctx, flush := perago.WithDeferred(ctx)` and call `flush()` when the work is done. Without a scope, the job runs right away in a goroutine. `DispatchAfterResponse` uses the same mechanism.

`background` runs each job in its own goroutine. `m.Wait()` and `m.Close()` wait for those goroutines.

## Failover

```go
"failover": {Driver: perago.FailoverDriver("redis", "database", "sync")},
```

The job goes to the first connection that accepts it, and `QueueFailedOver` fires for each one that fails. Run a worker for each underlying connection, except sync, background and deferred.

## Writing a driver

Implement `perago.Driver` (`Push`, `Pop`, `Size`, `Clear`). The optional extras are `BulkPusher`, `SizeReporter`, `AgeReporter` and `Closer`. Run `internal/drivertest.Run` against your driver: every built-in driver passes it.

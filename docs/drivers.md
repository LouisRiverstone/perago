---
title: Connections and drivers
nav_order: 3
description: "Database (every migro dialect), Redis, SQS, Beanstalkd, sync, deferred, background, failover, null and memory."
---

# Connections and drivers

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

A `perago.Connection` is one entry of Laravel's `config/queue.php`:

```go
perago.Connection{
	Driver:      database.NewDriver(db, database.DriverOptions{}),
	Queue:       "default", // the default queue
	AfterCommit: true,      // hold dispatches made inside a transaction until it commits
}
```

| Driver | Constructor | Notes |
|---|---|---|
| database | `database.NewDriver(db, opts)` | Every migro dialect. `retry_after` is `RetryAfter` (90s). |
| redis | `redis.NewDriver(client, opts)` | `go-redis` `UniversalClient`: single, Sentinel, Cluster. |
| sqs | `sqs.NewDriver(client, opts)` | Standard, FIFO and fair queues; overflow storage. |
| beanstalkd | `beanstalkd.NewDriver(conn, opts)` | `retry_after` is the job TTR. |
| sync | `perago.SyncDriver()` | Runs the job now and returns its error. |
| deferred | `perago.DeferredDriver()` | Runs after the current request, in this process. |
| background | `perago.BackgroundDriver()` | Runs in its own goroutine. |
| failover | `perago.FailoverDriver("redis", "database")` | Pushes to the first connection that takes the job. |
| null | `perago.Null()` | Discards jobs. |
| memory | `perago.NewMemoryDriver(retryAfter)` | A full queue in this process, for tests and single-process apps. |

## Database

```go
import _ "github.com/LouisRiverstone/migro/dialect/postgres" // or mysql, sqlite, sqlserver, oracle

db, _ := migro.Open(migro.Config{Dialect: "postgres", DSN: dsn})
d := database.NewDriver(db, database.DriverOptions{Table: "jobs", RetryAfter: 90 * time.Second})
```

Create the table with `perago make:queue-table` (a migro migration) or `database.Install`. The driver runs on PostgreSQL, CockroachDB, Redshift, MySQL, MariaDB, SQLite, SQL Server and Oracle. To reserve a job, it selects a few available rows and then claims one with a conditional `UPDATE`. That update succeeds only while the row is still free and unchanged, so two workers never take the same job, and no dialect needs `SKIP LOCKED`.

On MySQL and MariaDB the payload columns are `LONGTEXT`, so payloads are not capped at 64 KB.

{: .note }
SQLite allows one writer at a time. Give the DSN a busy timeout (`?_pragma=busy_timeout(10000)`) when several workers share a file.

{: .warning }
Redshift does not enforce unique keys, so `database.NewCache` cannot provide atomic locks there. On Redshift, keep the queue in the database and use a Redis cache.

## Redis

```go
rdb := goredis.NewUniversalClient(&goredis.UniversalOptions{Addrs: []string{"localhost:6379"}})
d := redis.NewDriver(rdb, redis.DriverOptions{
	Prefix:     "myapp:",          // one per application
	RetryAfter: 90 * time.Second,
	BlockFor:   5 * time.Second,   // BLPOP instead of polling (block_for)
})
```

A queue is a list of ready jobs plus sorted sets of delayed and reserved jobs. Lua scripts move jobs between them atomically, like Laravel's `RedisQueue`.

{: .warning }
On Redis Cluster, wrap queue names in a hash tag (`{default}`), so every key of a queue lands on the same slot. Give each application its own `Prefix` or database, or two apps will take each other's jobs.

## Amazon SQS

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
- **Overflow storage**: payloads of 1 MiB or more go to a store, and SQS carries a pointer:

```go
sqs.Options{Overflow: sqs.Overflow{
	Enabled: true, Store: sqs.CacheOverflow{Cache: redis.NewCache(rdb, "")},
	Always: false, DeleteAfterProcessing: true, FlushOnClear: false,
}}
```

## Beanstalkd

```go
conn, _ := beanstalkd.Dial("127.0.0.1:11300")
d := beanstalkd.NewDriver(conn, beanstalkd.Options{TTR: 90 * time.Second, BlockFor: 5 * time.Second})
```

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

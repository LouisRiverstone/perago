---
title: Home
nav_order: 1
description: "Laravel's queues for Go."
permalink: /
---

# perago

perago is [Laravel Queues](https://laravel.com/docs/queues) for Go. A job is a struct with a `Handle(ctx) error` method. You dispatch it to a connection, and a worker runs it with Laravel's semantics: tries, backoff, timeouts, middleware, unique and encrypted jobs, chains, batches, failed jobs, events, `queue:*` commands and test fakes.

The database driver and stores need nothing but `database/sql`, so they work on PostgreSQL, CockroachDB, Redshift, MySQL, MariaDB, SQLite, SQL Server and Oracle with any driver. perago works with [migro](https://github.com/LouisRiverstone/migro) through an optional module and never requires it ([Using with migro](migro.md)). Redis, Amazon SQS and Beanstalkd have their own drivers, each in its own module, so the core has no dependencies.

```go
type SendWelcome struct {
	perago.Queueable
	UserID int
}

func init() { perago.Register(&SendWelcome{}) }

func (j *SendWelcome) Handle(ctx context.Context) error { return mailer.Welcome(ctx, j.UserID) }

m.Dispatch(&SendWelcome{UserID: 42}).Delay(time.Minute).Send(ctx)
```

## Where to go next

| You want to | Read |
|---|---|
| Install perago and run your first worker | [Getting started](getting-started.md) |
| Pick and configure a backend | [Connections and drivers](drivers.md) |
| Use perago on a migro project (optional) | [Using with migro](migro.md) |
| Write jobs, and see how they are serialized | [Creating jobs](jobs.md) |
| Wrap jobs: rate limits, overlap locks, throttling | [Job middleware](middleware.md) |
| Dispatch: delays, sync, after commit, routing, closures | [Dispatching jobs](dispatching.md) |
| Run jobs in sequence | [Chains](chains.md) |
| Set tries, backoff, timeouts; release or fail by hand | [Attempts, timeouts and errors](retries.md) |
| Run a tracked group of jobs | [Batches](batches.md) |
| Run and deploy workers | [Running workers](workers.md) |
| Inspect and retry failures | [Failed jobs](failed-jobs.md) |
| Hook into the lifecycle, watch queue sizes | [Events and monitoring](events.md) |
| Test code that dispatches | [Testing](testing.md) |
| Look up a command | [Command line](command-line.md) |
| Find the perago page for a section of Laravel's docs | [Coming from Laravel](coming-from-laravel.md#laravels-queue-docs-section-by-section) |

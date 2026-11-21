<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="logo-dark.png">
    <img src="logo.png" alt="perago: queues for Go" width="520">
  </picture>
</p>

<p align="center">
  Laravel's queues for Go: jobs, workers, retries, middleware, chains, batches and failed jobs,<br>
  on any SQL database, Redis, Amazon SQS and Beanstalkd. Works with <a href="https://github.com/LouisRiverstone/migro">migro</a>, never requires it.
</p>

<p align="center">
  <a href="https://github.com/LouisRiverstone/perago/actions/workflows/ci.yml"><img src="https://github.com/LouisRiverstone/perago/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://pkg.go.dev/github.com/LouisRiverstone/perago"><img src="https://pkg.go.dev/badge/github.com/LouisRiverstone/perago.svg" alt="Go reference"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT license"></a>
  <a href="https://louisriverstone.github.io/perago/"><img src="https://img.shields.io/badge/docs-read-67c7dc.svg" alt="Documentation"></a>
</p>

---

**perago** brings [Laravel Queues](https://laravel.com/docs/queues) to Go. Each job is a struct with a `Handle` method. You dispatch it to a connection, and a worker runs it with Laravel's semantics for tries, backoff, timeouts, unique and encrypted jobs, rate limiting, chains, batches, failed jobs and `queue:*` commands. It ships with test fakes too. The database driver needs nothing but `database/sql`, so it works on PostgreSQL, CockroachDB, Redshift, MySQL, MariaDB, SQLite, SQL Server and Oracle with whatever driver you already use; on a [migro](https://github.com/LouisRiverstone/migro) project, the `perago/migro` module plugs it into your `*migro.DB`, migrations and models.

## Install

Go 1.25+.

```
go get github.com/LouisRiverstone/perago@latest
go install github.com/LouisRiverstone/perago/cmd/perago@latest
```

perago is split into modules, so you only download what you use. The core has no dependencies at all, and **migro is optional**: perago is compatible with it but never requires it. Add `perago/migro` only on a project that already uses migro ([Using with migro](https://louisriverstone.github.io/perago/migro.html)).

| Module | `go get` | Brings in |
|---|---|---|
| core | `github.com/LouisRiverstone/perago` | nothing: the standard library only. Jobs, workers, chains, batches, middleware, `queuetest`, `console`, the `database` driver and stores (`database/sql`) |
| Redis | `github.com/LouisRiverstone/perago/redis` | `go-redis` |
| SQS | `github.com/LouisRiverstone/perago/sqs` | AWS SDK v2 (SQS) |
| DynamoDB | `github.com/LouisRiverstone/perago/dynamodb` | AWS SDK v2 (DynamoDB) |
| Beanstalkd | `github.com/LouisRiverstone/perago/beanstalkd` | `go-beanstalk` |
| migro | `github.com/LouisRiverstone/perago/migro` | [migro](https://github.com/LouisRiverstone/migro): `Connect`, migrations, `model.Of` |

## A quick look

Generate a job and the queue tables (`.sql` files, or migro migrations on a migro project):

```
perago make:job ProcessPodcast
perago make:queue-table --dialect=postgres && perago make:queue-failed-table && perago make:queue-batches-table && perago make:cache-table
```

```go
// app/jobs/process_podcast.go
type ProcessPodcast struct {
	perago.Queueable
	PodcastID int64
}

func init() { perago.Register(&ProcessPodcast{}) }

func (j *ProcessPodcast) Tries() int                { return 5 }
func (j *ProcessPodcast) Backoff() []time.Duration  { return []time.Duration{10 * time.Second, time.Minute} }
func (j *ProcessPodcast) Middleware() []perago.Middleware {
	return []perago.Middleware{middleware.WithoutOverlapping(j.PodcastID)}
}

func (j *ProcessPodcast) Handle(ctx context.Context) error {
	// ... the error you return is retried with backoff until Tries runs out
	return nil
}
```

Configure the connections once, the way `config/queue.php` does. Redis is optional: the queue, the cache, failed jobs and batches can all live in your SQL database, and the worker can run inside your own process, so your app and its database are enough.

```go
sqlDB, _ := sql.Open("pgx", os.Getenv("DATABASE_URL")) // any database/sql driver
db, _ := database.New(sqlDB, "postgres")                // or peragomigro.Connect(migroDB)

m, err := perago.New(perago.Config{
	Default: "database",
	Connections: map[string]perago.Connection{
		"database": {Driver: database.NewDriver(db, database.DriverOptions{RetryAfter: 90 * time.Second}), AfterCommit: true},
		"redis":    {Driver: redis.NewDriver(rdb, redis.DriverOptions{BlockFor: 5 * time.Second})},
		"sync":     {Driver: perago.SyncDriver()},
	},
	Failed:  database.NewFailedJobStore(db, ""),
	Batches: database.NewBatchStore(db, ""),
	Cache:   redis.NewCache(rdb, ""),           // unique jobs, locks, rate limits, worker signals
})
```

Dispatch:

```go
m.Dispatch(&ProcessPodcast{PodcastID: podcast.ID}).OnQueue("podcasts").Delay(10 * time.Minute).Send(ctx)

m.Chain(&OptimizePodcast{}, &ReleasePodcast{}).Catch(func(ctx context.Context, err error) { ... }).Dispatch(ctx)

batch, _ := m.Batch(&ImportCsv{Rows: "1-100"}, &ImportCsv{Rows: "101-200"}).
	Then(func(ctx context.Context, b *perago.Batch) { ... }).
	AllowFailures().Dispatch(ctx)
```

Run workers like `artisan queue:work`, with an app console that embeds `console.App`:

```
perago queue:work redis --queue=high,default --tries=3 --timeout=60 --max-jobs=1000
perago queue:failed
perago queue:retry all
perago queue:monitor redis:default --max=100
```

Test without a queue:

```go
fake := queuetest.Fake(m)
service.Publish(ctx, podcast)
queuetest.AssertPushed(t, fake, func(j *ProcessPodcast) bool { return j.PodcastID == podcast.ID })
```

## What you get

- **Drivers**: database (`database/sql` on PostgreSQL, CockroachDB, Redshift, MySQL, MariaDB, SQLite, SQL Server, Oracle), Redis (single, Sentinel, Cluster), SQS (FIFO, fair queues, overflow storage), Beanstalkd, sync, deferred, background, failover, null and an in-memory queue
- **Jobs**: tries, backoff, timeouts, `RetryUntil`/`RetryFor`, max exceptions, fail on timeout, `Failed` hooks, manual `Release`/`Delete`/`Fail`, unique and unique-until-processing jobs, debounced jobs, encrypted jobs with key rotation, migro models serialized as keys (`perago/migro/model`)
- **Middleware**: `RateLimited`, `WithoutOverlapping`, `ThrottlesExceptions`, `FailOnException`, `Skip`, `Release`, `SkipIfBatchCancelled`, plus Redis variants
- **Dispatching**: delays, `DispatchSync`, after-response, bulk, `PrepareForDispatch`, after-commit transactions, queue routing and forwarding, closures
- **Chains and batches**: catch callbacks, prepend/append to chain, batches inside chains, then/catch/finally/progress, adding jobs, cancelling, `allowFailures`, retrying failed batch jobs, pruning, and DynamoDB storage
- **Workers**: priorities, `--once`, `--stop-when-empty`, `--max-jobs`, `--max-time`, `--memory`, `--rest`, `queue:restart`, `queue:pause`/`queue:resume`, graceful signals with `Interruptible` jobs, `queue:listen`
- **Failed jobs**: database, DynamoDB or disabled; `queue:failed`, `queue:retry`, `queue:retry-batch`, `queue:forget`, `queue:flush`, `queue:prune-failed`
- **Events and monitoring**: every Laravel queue event, `Before`/`After`/`Failing`/`Looping`, and `queue:monitor` with `QueueBusy`
- **Testing**: `Fake`, `FakeFor`, `FakeExcept` with every `Queue::`/`Bus::` assertion, and fake job interactions and batches

## Documentation

**<https://louisriverstone.github.io/perago/>**

## Contributing

Issues and pull requests are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md) first.

## License

[MIT](LICENSE)

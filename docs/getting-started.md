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

## Connections and queues

As in Laravel, a **connection** is a backend (a database, a Redis server, an SQS account) and a **queue** is a named line of jobs on it. Every connection has a default queue, `default` unless you set `Connection.Queue`. A job goes to the default connection and queue unless it says otherwise:

```go
m.Dispatch(&ProcessPodcast{}).Send(ctx)                    // default connection, default queue
m.Dispatch(&ProcessPodcast{}).OnQueue("emails").Send(ctx)  // the "emails" queue
```

Workers take jobs from the queues you name, in priority order: `queue:work --queue=high,default`.

## Create the tables (database driver)

The database driver needs a `jobs` table, failed jobs need `failed_jobs`, batches need `job_batches`, and the database cache needs `cache`. Generate migro migrations for them:

```
perago make:queue-table
perago make:queue-failed-table
perago make:queue-batches-table
perago make:cache-table
migro migration:run
```

Or create any missing tables at startup with `database.Install(ctx, db, database.Tables{})`.

## Build the manager

```go
import (
	"github.com/LouisRiverstone/migro"
	_ "github.com/LouisRiverstone/migro/dialect/postgres"
	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/database"
)

db, err := migro.Open(migro.Config{Dialect: "postgres", DSN: os.Getenv("DATABASE_URL")})

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

Or run one inside a service with `m.Work(ctx, perago.WorkerOptions{Queues: []string{"high", "default"}})`.

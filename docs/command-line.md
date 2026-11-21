---
title: Command line
nav_order: 15
description: "The perago CLI, embedding console.App, and every command."
---

# Command line

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Your application's console

Workers need your jobs and your connections, so the `queue:*` commands run inside your program. Embed `console.App`:

```go
// cmd/perago/main.go
package main

import (
	"context"
	"os"

	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/console"

	_ "example.com/app/app/jobs" // registers the jobs
	"example.com/app/internal/queue"
)

func main() {
	app := &console.App{
		Manager:     func(ctx context.Context) (*perago.Manager, error) { return queue.New(ctx) },
		Maintenance: func() bool { _, err := os.Stat("storage/down"); return err == nil },
	}
	os.Exit(app.Run(context.Background(), os.Args[1:]))
}
```

```
go run ./cmd/perago queue:work
```

Your own binary can also hand its arguments to `app.Run`, for example `myapp queue:work`.

## The perago CLI

`go install github.com/LouisRiverstone/perago/cmd/perago@latest`

- `make:*` commands run directly. They need no application and no database.
- Every other command is forwarded with `go run` to your application's console: `./cmd/perago`, or the package in `PERAGO_APP`. Signals are passed through.

## Commands

| Command | Laravel | Does |
|---|---|---|
| `queue:work [conn] [flags]` | same | Run a worker ([flags](workers.md#the-queuework-command)) |
| `queue:listen [conn] [flags]` | same | A fresh worker process per job |
| `queue:restart` | same | Workers exit after their current job |
| `queue:pause conn:queue` / `--all` | same | Stop taking jobs from a queue |
| `queue:resume conn:queue` / `--all`, `queue:continue` | same | Take jobs again |
| `queue:clear [conn] [--queue=] [--force]` | same | Delete a queue's jobs |
| `queue:monitor conn:queue,... [--max=1000]` | same | Sizes and `QueueBusy` |
| `queue:failed` | same | List failed jobs |
| `queue:retry <id...\|all> [--queue=]` | same | Retry failed jobs |
| `queue:retry-batch <id...>` | same | Retry a batch's failed jobs |
| `queue:forget <id>` | same | Delete a failed job |
| `queue:flush [--hours=]` | same | Delete failed jobs |
| `queue:prune-failed [--hours=24]` | same | Delete old failed jobs |
| `queue:prune-batches [--hours=24] [--unfinished=] [--cancelled=]` | same | Delete old batches |
| `make:job Name [--sync] [--batched]` | same | `app/jobs/name.go` |
| `make:job-middleware Name` | same | `app/jobs/middleware/name.go` |
| `make:queue-table` (`queue:table`) | same | Migration for `jobs` |
| `make:queue-failed-table` (`queue:failed-table`) | same | Migration for `failed_jobs` |
| `make:queue-batches-table` (`queue:batches-table`) | same | Migration for `job_batches` |
| `make:cache-table` | same | Migration for `cache` (locks, limits, signals) |

The `make:*-table` commands write a migration into `database/migrations` (`--table=` renames the table). On a project whose `go.mod` requires migro it is a migro migration, run with `migro migration:run`; anywhere else it is a `.sql` file for `--dialect` (default `postgres`). `--format=sql` or `--format=migro` overrides the choice. Output directories are `App.JobsDir`, `App.MiddlewareDir` and `App.MigrationsDir`.

When `Env` is `production` (from `PERAGO_ENV` or `APP_ENV`), destructive commands ask for confirmation, or need `--force` when not interactive.

Exit codes: `0` success, `1` error, `2` usage error. `queue:work` returns the worker status: `12` means it stopped at its memory limit.

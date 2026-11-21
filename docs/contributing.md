---
title: Contributing
nav_order: 17
description: "Set up, test against real backends, and add drivers."
---

# Contributing

```
git clone https://github.com/LouisRiverstone/perago.git && cd perago
gofmt -l . && for m in . redis sqs dynamodb beanstalkd migro internal/dbtest; do (cd $m && go vet ./... && go test -race -count=1 ./...); done
```

The repository holds several Go modules: the core at the root (no dependencies, checked by CI), `redis`, `sqs`, `dynamodb`, `beanstalkd`, `migro`, and `internal/dbtest`, which runs the database driver against real `database/sql` drivers. Each submodule points at the core next door with a `replace` directive, so changes to both are tested together. A release tags the core first (`v0.2.0`), then every module that changed with its directory as prefix (`redis/v0.2.0`, `migro/v0.2.0`).

The tests need nothing installed. The database tests use SQLite, the Redis tests use miniredis, and SQS, DynamoDB and Beanstalkd use in-memory fakes of their clients.

## Testing against a real database

```
PERAGO_TEST_DIALECT=postgres PERAGO_TEST_DSN="host=localhost user=postgres password=postgres dbname=perago sslmode=disable" \
  sh -c 'cd internal/dbtest && go test -p 1 -count=1 ./... && cd ../../migro && go test -p 1 -count=1 ./...'
```

The dialect names are the ones `database.New` accepts (and migro's): postgres, cockroachdb, redshift, mysql, mariadb, sqlite, sqlserver, oracle. CI runs PostgreSQL, MySQL and SQLite as blocking jobs, and MariaDB, CockroachDB, SQL Server and a live Beanstalkd as non-blocking ones.

## Adding a driver or a cache

A driver implements `perago.Driver` and must pass `internal/drivertest.Run`. A cache implements `perago.Cache` and must pass `internal/cachetest.Run`. A full backend (driver, cache and stores) must also pass `internal/e2etest.Run`: Laravel's queue scenarios (retries and backoff, failed job retry, release, timeouts, chains, batches, unique jobs, rate limits, overlap locks, encryption, pause and restart, priorities, concurrent workers) run through the Manager and a real worker. The memory, database (every dialect, with and without migro) and Redis backends all run it, and `internal/dbtest` also drives the `queue:*` commands against the database. Both suites accept a clock you can advance, so time-based cases run instantly.

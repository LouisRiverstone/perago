---
title: Contributing
nav_order: 16
description: "Set up, test against real backends, and add drivers."
---

# Contributing

```
git clone https://github.com/LouisRiverstone/perago.git && cd perago
gofmt -l . && go vet ./... && go test -race -count=1 ./...
```

The tests need nothing installed. The database tests use SQLite, the Redis tests use miniredis, and SQS, DynamoDB and Beanstalkd use in-memory fakes of their clients.

## Testing against a real database

```
PERAGO_TEST_DIALECT=postgres PERAGO_TEST_DSN="host=localhost user=postgres password=postgres dbname=perago sslmode=disable" \
  go test -p 1 -count=1 ./database/... ./model/...
```

The dialect names are migro's: postgres, cockroachdb, redshift, mysql, mariadb, sqlite, sqlserver, oracle. CI runs PostgreSQL, MySQL and SQLite as blocking jobs, and MariaDB, CockroachDB, SQL Server and a live Beanstalkd as non-blocking ones.

## Adding a driver or a cache

A driver implements `perago.Driver` and must pass `internal/drivertest.Run`. A cache implements `perago.Cache` and must pass `internal/cachetest.Run`. Both suites accept a clock you can advance, so time-based cases run instantly.

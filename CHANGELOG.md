# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Jobs with Laravel's options: tries, backoff, timeouts, `RetryUntil`/`RetryFor`, max exceptions, fail on timeout, crash counting, `Failed` hooks, manual release/delete/fail, unique, unique-until-processing, debounced and encrypted jobs (with key rotation), `PrepareForDispatch`, `Interruptible`.
- Drivers: database on every migro dialect, Redis, SQS (FIFO, fair queues, overflow, long delays), Beanstalkd, sync, deferred, background, failover, null and memory.
- Dispatching: delays, sync, after-response, bulk, after-commit transactions, routing, forwarding, closures and named closures.
- Chains with catch callbacks and prepend/append; batches with before/progress/then/catch/finally, adding jobs, cancellation, allowed failures, retries, pruning, chains in batches and batches in chains.
- Middleware: RateLimited, WithoutOverlapping, ThrottlesExceptions, FailOnException, Skip, Release, SkipIfBatchCancelled.
- Workers with every `queue:work` flag, signals, pause/resume, restart and memory limits; failed job stores for the database and DynamoDB; batch stores for the database and DynamoDB; database and Redis caches.
- `model.Of` / `model.Many`: migro models serialized as keys.
- `queuetest`: Queue::fake / Bus::fake with every assertion, fake job interactions and fake batches.
- Console with every `queue:*` and `make:*` command, and the `perago` CLI.

---
title: Errors
nav_order: 15
description: "perago's error type and codes."
---

# Errors

Every error perago returns across a package boundary is a `*perago.Error` with a stable `Code`:

```go
_, err := m.Dispatch(job).OnConnection("nope").Send(ctx)
if errors.Is(err, perago.ErrConnectionNotFound) { ... }
if perago.CodeOf(err) == perago.ConnectionNotFound { ... }
if perago.IsCode(err, perago.BatchNotFound) { ... }
```

| Code | Meaning |
|---|---|
| `INVALID_CONFIG` | Missing or wrong configuration (no key for encrypted jobs, no batch store, ...) |
| `CONNECTION_NOT_FOUND` | Unknown connection name |
| `JOB_NOT_REGISTERED` | A worker popped a job type it does not know (`perago.Register`) |
| `PAYLOAD_INVALID` | A payload that cannot be decoded |
| `PAYLOAD_DECRYPT_FAILED` | An encrypted payload that no configured key opens |
| `CLOSURE_NOT_SERIALIZABLE` | A `perago.Closure` sent to a connection served by other processes |
| `PUSH_FAILED`, `POP_FAILED`, `DRIVER_FAILED` | The backend failed |
| `JOB_FAILED_MANUALLY` | `Fail()` without a reason |
| `JOB_TIMEOUT` | The job outlived its timeout |
| `MAX_ATTEMPTS_EXCEEDED` | The job came back after its last attempt or its retry deadline |
| `MAX_EXCEPTIONS_EXCEEDED` | The job hit `MaxExceptions` |
| `BATCH_NOT_FOUND`, `FAILED_JOB_NOT_FOUND` | Unknown id |
| `STORE_FAILED`, `CACHE_FAILED` | A failed job, batch or cache store failed |
| `FAILOVER_EXHAUSTED` | Every failover connection rejected the job |
| `MODEL_NOT_FOUND` | A `model.Of` field's row is gone |
| `NOT_IN_JOB` | A runtime method called outside a running job |

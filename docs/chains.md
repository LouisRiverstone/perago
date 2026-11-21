---
title: Chains
nav_order: 8
description: "Run jobs one after another; catch failures; change the chain from inside a job."
---

# Chains

```go
_, err := m.Chain(
	&ProcessPodcast{ID: 1},
	&OptimizePodcast{ID: 1},
	perago.Closure(func(ctx context.Context) error { ... }), // in-process connections only
).OnConnection("redis").OnQueue("podcasts").
	Catch(func(ctx context.Context, err error) {
		// a job of the chain failed for good
	}).
	Dispatch(ctx)
```

Each job runs only after the previous one succeeded. When a job fails, the rest of the chain is dropped and the `Catch` callbacks run. A job that deletes itself (`j.Delete()`) does not stop the chain. `OnConnection` and `OnQueue` apply to every job that does not choose its own.

`Catch` takes a Go closure, so it runs only when the failing job's worker is this process. With workers in other processes, use `CatchJob(&NotifyChainFailed{})`, a registered job that reads the failure with `perago.ErrorFrom(ctx)`.

`m.Dispatch(job).Chain(next...)` is the same as `withChain`.

## Changing the chain from a job

```go
func (j *ProcessPodcast) Handle(ctx context.Context) error {
	if j.NeedsTranscript {
		return j.PrependToChain(&Transcribe{ID: j.ID}) // runs next
	}
	return j.AppendToChain(&Notify{ID: j.ID})         // runs last
}
```

## Batches inside chains

A `*PendingBatch` can be a link of the chain. The chain waits for the batch to finish, and stops if the batch is cancelled:

```go
m.Chain(
	&FlushCache{},
	m.Batch(&ReleasePodcast{ID: 1}, &ReleasePodcast{ID: 2}),
	&SendNotifications{},
).Dispatch(ctx)
```

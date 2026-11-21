---
title: Chains
nav_order: 8
description: "Run jobs one after another: connection and queue, adding jobs from inside the chain, failures, batches in chains."
---

# Chains

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

A chain is a list of jobs that run in sequence. Each one is queued only after the previous one succeeded. `Bus::chain` is `m.Chain`:

```go
_, err := m.Chain(
	&ProcessPodcast{ID: 1},
	&OptimizePodcast{ID: 1},
	&ReleasePodcast{ID: 1},
).Dispatch(ctx)
```

The chain travels with its first job: the rest of the jobs are stored in its payload (`chain`), and the worker that finishes a job dispatches the next one. No extra table is needed. `Dispatch` returns the UUID of the first job.

A [closure](dispatching.md#queueing-closures) can be a link of the chain on in-process connections, and `perago.Call` on any connection:

```go
m.Chain(
	&ProcessPodcast{ID: 1},
	perago.Call("podcasts.notify", map[string]int64{"ID": 1}),
).Dispatch(ctx)
```

`m.Dispatch(job).Chain(next...)` is the same as Laravel's `withChain`.

{: .note }
A job that deletes itself with `j.Delete()` does not stop the chain: the next job still runs. The chain stops only when a job **fails**, that is, when its attempts run out or it calls `Fail`.

## Chain connection and queue

`OnConnection` and `OnQueue` apply to every job of the chain that does not pick its own; `Delay` delays the first one:

```go
m.Chain(&ProcessPodcast{ID: 1}, &OptimizePodcast{ID: 1}).
	OnConnection("redis").
	OnQueue("podcasts").
	Dispatch(ctx)
```

## Adding jobs to the chain

A job that is running can change the rest of its chain:

```go
func (j *ProcessPodcast) Handle(ctx context.Context) error {
	if j.NeedsTranscript {
		if err := j.PrependToChain(&TranscribePodcast{ID: j.ID}); err != nil { // runs next
			return err
		}
	}
	return j.AppendToChain(&NotifySubscribers{ID: j.ID}) // runs after the last one
}
```

Both work in a job that was not part of a chain too: it becomes the start of one.

## Chain failures

When a job of the chain fails, the remaining jobs are dropped and the chain's `Catch` callbacks run with the error:

```go
m.Chain(&ProcessPodcast{ID: 1}, &OptimizePodcast{ID: 1}).
	Catch(func(ctx context.Context, err error) {
		slog.Error("podcast pipeline failed", "err", err)
	}).
	Dispatch(ctx)
```

Laravel serializes the `catch` closure and runs it on whichever worker sees the failure. A Go closure cannot travel, so a `Catch` closure runs only when that worker is the process that dispatched the chain (an application running its workers in-process). With workers in other processes, use `CatchJob`: a registered job that travels in the payload and that the failing worker runs, wherever it is. It reads the error with `perago.ErrorFrom(ctx)`:

```go
type NotifyChainFailed struct {
	perago.Queueable
	PodcastID int64
}

func (j *NotifyChainFailed) Handle(ctx context.Context) error {
	return alerts.Send(ctx, "podcast %d: %v", j.PodcastID, perago.ErrorFrom(ctx))
}

m.Chain(&ProcessPodcast{ID: 1}, &OptimizePodcast{ID: 1}).
	CatchJob(&NotifyChainFailed{PodcastID: 1}).
	Dispatch(ctx)
```

The failing job's own `Failed` method runs too, and the job is stored in `failed_jobs`.

## Batches inside chains

A `*PendingBatch` can be a link of the chain. The chain waits for the whole batch to finish before it goes on, and stops if the batch is cancelled:

```go
m.Chain(
	&FlushPodcastCache{},
	m.Batch(&ReleasePodcast{ID: 1}, &ReleasePodcast{ID: 2}),
	m.Batch(&SendPodcastReleaseNotification{ID: 1}, &SendPodcastReleaseNotification{ID: 2}),
).Dispatch(ctx)
```

The reverse, chains inside a batch, is covered in [Batches](batches.md#chains-and-batches).

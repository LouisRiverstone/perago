---
title: Testing
nav_order: 12
description: "Queue::fake, Bus::fake, chain and batch assertions, fake job interactions and fake batches."
---

# Testing

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Faking the queue

`queuetest.Fake(m)` captures every dispatch of `m` instead of queueing it. It is Laravel's `Queue::fake` and `Bus::fake` in one:

```go
func TestShip(t *testing.T) {
	fake := queuetest.Fake(m)
	defer fake.Restore()

	service.Ship(ctx, order)

	queuetest.AssertPushed(t, fake, func(j *ShipOrder) bool { return j.OrderID == order.ID })
	queuetest.AssertPushedOn[*ShipOrder](t, fake, "shipping")
	queuetest.AssertPushedTimes[*SendReceipt](t, fake, 1)
	queuetest.AssertPushedOnce[*SendReceipt](t, fake)
	queuetest.AssertNotPushed[*Refund](t, fake)
	queuetest.AssertCount(t, fake, 2)
	queuetest.AssertNothingPushed(t, other)
	queuetest.AssertClosurePushed(t, fake)
	queuetest.AssertClosureNotPushed(t, fake)
	queuetest.AssertDispatchedSync[*Audit](t, fake)
	queuetest.AssertDispatchedAfterResponse[*Audit](t, fake)
}
```

`AssertDispatched`, `AssertNotDispatched`, `AssertDispatchedTimes` and `AssertNothingDispatched` are the `Bus::fake` names for the same checks. Use `fake.Pushed()` to make your own assertions.

## Faking a subset of jobs

```go
fake := queuetest.FakeFor(m, &ShipOrder{})       // only these are captured; the rest run for real
fake := queuetest.FakeExcept(m, &AuditLog{})     // everything but these is captured
```

## Testing job chains

```go
queuetest.AssertChained(t, fake,
	&ProcessPodcast{},                                     // by type
	func(j *OptimizePodcast) bool { return j.ID == 1 },   // by predicate
	queuetest.ChainedBatch(func(b queuetest.Batched) bool { return len(b.Jobs) == 3 }),
)
queuetest.AssertDispatchedWithoutChain[*ProcessPodcast](t, fake)
```

### Testing chain modifications

```go
job := queuetest.WithFakeQueueInteractions(&ProcessPodcast{NeedsTranscript: true})
_ = job.Handle(ctx)
job.AssertHasChain(t, &Transcribe{})
job.AssertDoesntHaveChain(t) // or this
```

## Testing job batches

```go
queuetest.AssertBatched(t, fake, func(b queuetest.Batched) bool {
	return b.Name == "Import CSV" && b.HasJobs(&ImportCsv{}, &ImportCsv{})
})
queuetest.AssertBatchCount(t, fake, 1)
queuetest.AssertNothingBatched(t, fake)
```

### Testing job and batch interaction

```go
job, batch, rec := queuetest.WithFakeBatch(&LoadImportBatch{})
_ = job.Handle(ctx)
rec.Added      // the jobs the job added with batch.Add
rec.Cancelled  // whether it cancelled the batch
_ = batch
```

## Testing job and queue interactions

```go
job := queuetest.WithFakeQueueInteractions(&CheckPayment{PaymentID: 1}, 2) // attempt 2
_ = job.Handle(ctx)

job.AssertReleased(t, 30*time.Second)
job.AssertNotReleased(t)
job.AssertDeleted(t)
job.AssertNotDeleted(t)
job.AssertFailed(t)
job.AssertFailedWith(t, ErrDeclined)       // errors.Is, or a message substring
job.AssertNotFailed(t)
```

## Running jobs for real in tests

`perago.NewMemoryDriver` and the memory stores give you a real queue in the test process. Use `m.Work(ctx, perago.WorkerOptions{StopWhenEmpty: true})` to drain it.

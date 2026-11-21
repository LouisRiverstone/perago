---
title: Testing
nav_order: 14
description: "Queue::fake and Bus::fake, assertions, faking a subset of jobs, chain and batch assertions, fake job interactions and fake batches."
---

# Testing

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

When you test code that dispatches jobs, you usually want to check that the right jobs were dispatched, not run them. Test the jobs themselves separately, by building one and calling `Handle`.

`queuetest.Fake(m)` captures every dispatch of the Manager instead of queueing it. It is Laravel's `Queue::fake` and `Bus::fake` in one, and covers dispatches, chains and batches:

```go
func TestShipOrder(t *testing.T) {
	fake := queuetest.Fake(m)
	defer fake.Restore()

	if err := orders.Ship(ctx, order); err != nil {
		t.Fatal(err)
	}

	queuetest.AssertNothingPushed(t, fake)                       // or, after a dispatch:
	queuetest.AssertPushed[*ShipOrder](t, fake)                  // pushed at least once
	queuetest.AssertPushedOn[*ShipOrder](t, fake, "shipping")    // ... on this queue
	queuetest.AssertPushedOnce[*ShipOrder](t, fake)              // exactly once
	queuetest.AssertPushedTimes[*ShipOrder](t, fake, 2)          // exactly twice
	queuetest.AssertNotPushed[*AnotherJob](t, fake)
	queuetest.AssertClosurePushed(t, fake)
	queuetest.AssertClosureNotPushed(t, fake)
	queuetest.AssertCount(t, fake, 3)                            // three jobs in total
}
```

The type parameter is the job type, so a typo is a compile error rather than a passing test.

Every assertion takes optional predicates, which play the part of Laravel's closure truth tests and of its expected-properties arrays. A dispatch matches when every predicate returns true:

```go
queuetest.AssertPushed(t, fake, func(j *ShipOrder) bool {
	return j.OrderID == order.ID
})

queuetest.AssertPushedOn(t, fake, "shipping", func(j *ShipOrder) bool {
	return j.OrderID == order.ID && j.Express
})
```

`fake.Pushed()` returns every captured dispatch, with its job, connection, queue, delay and chain, for checks of your own:

```go
for _, p := range fake.Pushed() {
	if p.Delay < time.Minute {
		t.Errorf("%T dispatched with a delay of %s", p.Job, p.Delay)
	}
}
```

The `Bus::fake` names work too: `AssertDispatched`, `AssertNotDispatched`, `AssertDispatchedTimes` and `AssertNothingDispatched`, plus `AssertDispatchedSync` and `AssertDispatchedAfterResponse` for `DispatchSync` and `DispatchAfterResponse`:

```go
queuetest.AssertDispatchedSync[*AuditLog](t, fake)
queuetest.AssertDispatchedAfterResponse(t, fake, func(j *RecordVisit) bool { return j.Path == "/" })
```

## Faking a subset of jobs

To fake only some jobs and let the others run as usual, list them:

```go
fake := queuetest.FakeFor(m, &ShipOrder{})      // only ShipOrder is captured
fake := queuetest.FakeExcept(m, &AuditLog{})    // everything but AuditLog is captured
```

## Testing job chains

`AssertChained` checks a chain. Give each link as a job (its type is compared), a predicate, or a `ChainedBatch`:

```go
queuetest.AssertChained(t, fake,
	&ShipOrder{},
	&RecordShipment{},
	func(j *UpdateInventory) bool { return j.OrderID == order.ID },
)
```

`AssertDispatchedWithoutChain` checks that a job was dispatched on its own:

```go
queuetest.AssertDispatchedWithoutChain[*ShipOrder](t, fake)
```

### Testing chain modifications

When a job [changes its chain](chains.md#adding-jobs-to-the-chain), run it with fake queue interactions and assert on the result:

```go
job := queuetest.WithFakeQueueInteractions(&ProcessPodcast{NeedsTranscript: true})
if err := job.Handle(ctx); err != nil {
	t.Fatal(err)
}

job.AssertHasChain(t, &TranscribePodcast{}, &OptimizePodcast{}, &ReleasePodcast{})
job.AssertDoesntHaveChain(t) // or, when the chain should be empty
```

### Testing chained batches

When a chain contains a batch, match it with `ChainedBatch`:

```go
queuetest.AssertChained(t, fake,
	&FlushPodcastCache{},
	queuetest.ChainedBatch(func(b queuetest.Batched) bool { return len(b.Jobs) == 3 }),
	&SendPodcastReleaseNotification{},
)
```

## Testing job batches

`AssertBatched` checks a dispatched batch. A `queuetest.Batched` has the batch's `Name`, `Jobs`, `Connection`, `Queue` and `AllowFailures`:

```go
queuetest.AssertBatched(t, fake, func(b queuetest.Batched) bool {
	return b.Name == "Import CSV" && len(b.Jobs) == 10
})

queuetest.AssertBatchCount(t, fake, 3)
queuetest.AssertNothingBatched(t, fake)
```

`HasJobs` checks every job of the batch, in order, so give one entry per job. Each entry can be a job (compared by type), or a predicate:

```go
queuetest.AssertBatched(t, fake, func(b queuetest.Batched) bool {
	return b.HasJobs(
		&ImportCsv{},
		func(j *ImportCsv) bool { return j.From == 101 },
	)
})
```

### Testing job and batch interaction

To test what a job does to its batch (cancel it, add jobs), give it a fake batch:

```go
job, batch, rec := queuetest.WithFakeBatch(&LoadImportBatch{Page: 1})
if err := job.Handle(ctx); err != nil {
	t.Fatal(err)
}

if rec.Cancelled {
	t.Error("the batch was cancelled")
}
if len(rec.Added) != 1000 {
	t.Errorf("added %d jobs", len(rec.Added))
}
_ = batch // the *perago.Batch the job saw
```

## Testing job and queue interactions

To test that a job releases, deletes or fails itself, run it with fake queue interactions. The optional argument sets the attempt number:

```go
job := queuetest.WithFakeQueueInteractions(&CheckPayment{PaymentID: 1}, 2) // attempt 2
if err := job.Handle(ctx); err != nil {
	t.Fatal(err)
}

job.AssertReleased(t, 30*time.Second) // released, with this delay
job.AssertNotReleased(t)
job.AssertDeleted(t)
job.AssertNotDeleted(t)
job.AssertFailed(t)
job.AssertFailedWith(t, ErrDeclined)  // an error (errors.Is) or a message substring
job.AssertNotFailed(t)
```

## Running jobs for real in tests

For an end-to-end test, give the Manager a real queue in the test process and drain it:

```go
m, _ := perago.New(perago.Config{
	Default:     "memory",
	Connections: map[string]perago.Connection{"memory": {Driver: perago.NewMemoryDriver(time.Minute)}},
	Failed:      perago.NewMemoryFailedJobStore(),
	Batches:     perago.NewMemoryBatchStore(),
})

_, _ = m.Dispatch(&ShipOrder{OrderID: 1}).Send(ctx)
_, _ = m.Work(ctx, perago.WorkerOptions{StopWhenEmpty: true})
```

The `sync` driver is the shortest path when you just want the job to run at dispatch time. To test against your real database, point `database.New` at it and use the same worker loop.

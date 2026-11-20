// Package cachetest is the conformance suite every perago.Cache passes.
package cachetest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/LouisRiverstone/perago"
)

// Factory returns a fresh cache and a function moving its clock forward (nil to sleep).
type Factory func(t *testing.T) (c perago.Cache, advance func(time.Duration))

func pass(advance func(time.Duration), d time.Duration) {
	if advance != nil {
		advance(d)
		return
	}
	time.Sleep(d)
}

// Run runs the suite.
func Run(t *testing.T, newCache Factory) {
	ctx := context.Background()

	t.Run("get put forget", func(t *testing.T) {
		c, _ := newCache(t)
		if _, ok, err := c.Get(ctx, "k"); ok || err != nil {
			t.Fatalf("missing key: %v %v", ok, err)
		}
		if err := c.Put(ctx, "k", "v1", 0); err != nil {
			t.Fatal(err)
		}
		if err := c.Put(ctx, "k", "v2", 0); err != nil {
			t.Fatal(err)
		}
		if v, ok, _ := c.Get(ctx, "k"); !ok || v != "v2" {
			t.Fatalf("get = %q %v", v, ok)
		}
		if err := c.Forget(ctx, "k"); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := c.Get(ctx, "k"); ok {
			t.Fatal("forgotten key still there")
		}
		if err := c.Forget(ctx, "never"); err != nil {
			t.Fatalf("forget missing: %v", err)
		}
	})

	t.Run("ttl", func(t *testing.T) {
		c, advance := newCache(t)
		if err := c.Put(ctx, "t", "v", time.Second); err != nil {
			t.Fatal(err)
		}
		pass(advance, 2*time.Second)
		if _, ok, _ := c.Get(ctx, "t"); ok {
			t.Fatal("expired key still there")
		}
	})

	t.Run("add is atomic", func(t *testing.T) {
		c, advance := newCache(t)
		var wg sync.WaitGroup
		var mu sync.Mutex
		won := 0
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ok, err := c.Add(ctx, "lock", "x", time.Second)
				if err != nil {
					t.Error(err)
				}
				if ok {
					mu.Lock()
					won++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if won != 1 {
			t.Fatalf("%d goroutines acquired the lock", won)
		}
		pass(advance, 2*time.Second)
		if ok, _ := c.Add(ctx, "lock", "y", time.Second); !ok {
			t.Fatal("expired lock not acquirable")
		}
	})

	t.Run("increment", func(t *testing.T) {
		c, _ := newCache(t)
		var wg sync.WaitGroup
		for range 5 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := c.Increment(ctx, "n", 2, time.Minute); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		v, ok, _ := c.Get(ctx, "n")
		if !ok || v != "10" {
			t.Fatalf("counter = %q", v)
		}
	})

	t.Run("forget if", func(t *testing.T) {
		c, _ := newCache(t)
		_ = c.Put(ctx, "owner", "a", 0)
		if ok, _ := c.ForgetIf(ctx, "owner", "b"); ok {
			t.Fatal("released a lock held by someone else")
		}
		if ok, _ := c.ForgetIf(ctx, "owner", "a"); !ok {
			t.Fatal("owner could not release")
		}
		if _, ok, _ := c.Get(ctx, "owner"); ok {
			t.Fatal("lock still held")
		}
	})

	t.Run("lock", func(t *testing.T) {
		c, _ := newCache(t)
		a := perago.NewLock(c, "res", "", time.Minute)
		b := perago.NewLock(c, "res", "", time.Minute)
		if ok, _ := a.Get(ctx); !ok {
			t.Fatal("a")
		}
		if ok, _ := b.Get(ctx); ok {
			t.Fatal("b acquired a held lock")
		}
		if ok, _ := b.Release(ctx); ok {
			t.Fatal("b released a's lock")
		}
		if ok, _ := a.Release(ctx); !ok {
			t.Fatal("a release")
		}
		if ok, _ := b.Get(ctx); !ok {
			t.Fatal("b after release")
		}
	})
}

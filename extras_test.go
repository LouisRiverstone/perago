package perago_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/LouisRiverstone/perago"
)

func TestDispatchCatch(t *testing.T) {
	e := newEnv(t)
	var caught atomic.Value
	_, err := e.m.Dispatch(&Flaky{Name: "catch-me", FailTimes: 9, TriesN: 1}).
		Catch(func(ctx context.Context, err error) { caught.Store(err.Error()) }).Send(ctx)
	if err != nil {
		t.Fatal(err)
	}
	e.drain(t, perago.WorkerOptions{})
	if v, _ := caught.Load().(string); !strings.Contains(v, "boom catch-me") {
		t.Fatalf("catch = %q", v)
	}
}

func TestPauseAllKeepsIndividualPauses(t *testing.T) {
	e := newEnv(t)
	_ = e.m.Pause(ctx, "memory", "solo", 0)
	_ = e.m.PauseAll(ctx, 0)
	if p, _ := e.m.IsPaused(ctx, "memory", "default"); !p {
		t.Fatal("PauseAll did not pause")
	}
	_ = e.m.ResumeAll(ctx)
	if p, _ := e.m.IsPaused(ctx, "memory", "default"); p {
		t.Fatal("ResumeAll did not resume")
	}
	if p, _ := e.m.IsPaused(ctx, "memory", "solo"); !p {
		t.Fatal("ResumeAll resumed an individually paused queue")
	}
}

func TestWithoutInterruptionPolling(t *testing.T) {
	e := newEnv(t)
	e.m.WithoutInterruptionPolling()
	_ = e.m.Pause(ctx, "", "", 0)
	_, _ = e.m.Dispatch(&Echo{Msg: "polled-not"}).Send(ctx)
	_, _ = e.m.Work(ctx, perago.WorkerOptions{StopWhenEmpty: true})
	if got := takeRuns(); len(got) != 1 {
		t.Fatalf("worker honoured pause without polling: %v", got)
	}
}

func TestBatchJSON(t *testing.T) {
	e := newEnv(t)
	b, err := e.m.Batch(&Echo{Msg: "j"}).Name("export").Dispatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(b)
	var got map[string]any
	_ = json.Unmarshal(raw, &got)
	if got["name"] != "export" || got["totalJobs"].(float64) != 1 || got["progress"].(float64) != 0 || got["finishedAt"] != nil {
		t.Fatalf("json = %s", raw)
	}
}

var errNoAccess = errors.New("no access")

type Fragile struct{}

func (Fragile) Handle(context.Context) error { return errNoAccess }
func (Fragile) Tries() int                   { return 5 }

func init() { perago.Register(&Fragile{}) }

func TestKeyRotation(t *testing.T) {
	old := newEnv(t)
	_, _ = old.m.Dispatch(&Secret{Card: "rotated"}).Send(ctx)
	raw := old.q.Payloads("default")[0]
	rotated := newEnv(t, func(c *perago.Config) {
		c.PreviousKeys = [][]byte{c.Key}
		c.Key = []byte("ffffffffffffffffffffffffffffffff")
	})
	_, _ = rotated.q.Push(ctx, perago.Message{Queue: "default", Payload: raw})
	rotated.drain(t, perago.WorkerOptions{})
	if got := takeRuns(); len(got) != 1 || got[0] != "card rotated" {
		t.Fatalf("runs = %v", got)
	}
	wrong := newEnv(t, func(c *perago.Config) { c.Key = []byte("ffffffffffffffffffffffffffffffff") })
	_, _ = wrong.q.Push(ctx, perago.Message{Queue: "default", Payload: raw})
	wrong.drain(t, perago.WorkerOptions{})
	all, _ := wrong.failed.All(ctx)
	if len(all) != 1 || !strings.Contains(all[0].Exception, "PAYLOAD_DECRYPT_FAILED") {
		t.Fatalf("failed = %+v", all)
	}
}

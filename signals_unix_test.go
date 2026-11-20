//go:build !windows

package perago_test

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/LouisRiverstone/perago"
)

type LongImport struct{}

var interrupted = make(chan os.Signal, 1)

func (LongImport) Handle(ctx context.Context) error {
	select {
	case <-interrupted:
		record("stopped early")
	case <-time.After(5 * time.Second):
	}
	return nil
}

func (LongImport) Interrupted(sig os.Signal) { interrupted <- sig }

func init() { perago.Register(&LongImport{}) }

func TestSignalsInterruptAndStop(t *testing.T) {
	e := newEnv(t)
	_, _ = e.m.Dispatch(&LongImport{}).Send(ctx)
	_, _ = e.m.Dispatch(&Echo{Msg: "never"}).Send(ctx)
	perago.Listen(e.m, func(ctx context.Context, ev perago.JobProcessing) {
		go func() {
			time.Sleep(50 * time.Millisecond)
			_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		}()
	})
	done := make(chan int, 1)
	go func() {
		code, _ := e.m.Work(ctx, perago.WorkerOptions{HandleSignals: true, Sleep: time.Millisecond, Timeout: -1})
		done <- code
	}()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit %d", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker ignored SIGTERM")
	}
	if got := takeRuns(); len(got) != 1 || got[0] != "stopped early" {
		t.Fatalf("runs = %v", got)
	}
}

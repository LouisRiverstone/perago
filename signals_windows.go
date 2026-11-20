//go:build windows

package perago

import (
	"os"
	"os/signal"
)

func listenSignals(st *workerState) func() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case s := <-ch:
			st.quit.Store(true)
			st.interrupt(s)
			st.notify()
		}
	}()
	return func() {
		signal.Stop(ch)
		close(done)
	}
}

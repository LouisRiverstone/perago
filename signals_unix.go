//go:build !windows

package perago

import (
	"os"
	"os/signal"
	"syscall"
)

func listenSignals(st *workerState) func() {
	ch := make(chan os.Signal, 4)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGUSR2, syscall.SIGCONT)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case s := <-ch:
				switch s {
				case syscall.SIGUSR2:
					st.paused.Store(true)
				case syscall.SIGCONT:
					st.paused.Store(false)
				default:
					st.quit.Store(true)
					st.interrupt(s)
				}
				st.notify()
			}
		}
	}()
	return func() {
		signal.Stop(ch)
		close(done)
	}
}

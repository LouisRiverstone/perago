// Command perago runs the make:* generators anywhere and forwards every other command to your
// application's console (a main package that runs console.App), found at ./cmd/perago or at the
// path in PERAGO_APP, through `go run`.
//
//	go install github.com/LouisRiverstone/perago/cmd/perago@latest
//	perago make:job ProcessPodcast
//	perago queue:work redis --queue=high,default
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/LouisRiverstone/perago/console"
)

// version is set by the release build.
var version = "dev"

func findRoot(dir string) (string, error) {
	for d := dir; ; d = filepath.Dir(d) {
		if _, err := os.Stat(filepath.Join(d, "go.mod")); err == nil {
			return d, nil
		}
		if filepath.Dir(d) == d {
			return "", errors.New("go.mod not found")
		}
	}
}

// appPath is the application's console package, relative to root.
func appPath(root string) (string, bool) {
	p := os.Getenv("PERAGO_APP")
	if p == "" {
		p = "./cmd/perago"
	}
	if !strings.HasPrefix(p, ".") && !filepath.IsAbs(p) {
		p = "./" + p
	}
	if _, err := os.Stat(filepath.Join(root, p)); err != nil {
		return p, false
	}
	return p, true
}

func run(args []string) int {
	if len(args) > 0 && (args[0] == "--version" || args[0] == "version") {
		fmt.Println("perago", version)
		return 0
	}
	cwd, _ := os.Getwd()
	root, err := findRoot(cwd)
	if err != nil {
		root = cwd
	}
	if len(args) == 0 || strings.HasPrefix(args[0], "make:") || strings.HasSuffix(args[0], "-table") || args[0] == "list" || args[0] == "help" {
		app := &console.App{Root: root}
		return app.Run(context.Background(), args)
	}
	p, ok := appPath(root)
	if !ok {
		fmt.Fprintf(os.Stderr, "perago: %s needs your application's console.\n"+
			"Create %s with a main that runs console.App (see https://louisriverstone.github.io/perago/command-line/),\n"+
			"or set PERAGO_APP to its package path.\n", args[0], p)
		return console.ExitUsage
	}
	cmd := exec.Command("go", append([]string{"run", p}, args...)...)
	cmd.Dir = root
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	sigs := make(chan os.Signal, 4)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigs)
	if err := cmd.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "perago:", err)
		return console.ExitError
	}
	go func() {
		for s := range sigs {
			if cmd.Process != nil {
				_ = cmd.Process.Signal(s)
			}
		}
	}()
	err = cmd.Wait()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return exit.ExitCode()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "perago:", err)
		return console.ExitError
	}
	return console.ExitOK
}

func main() { os.Exit(run(os.Args[1:])) }

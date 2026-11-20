// Package console is the artisan-style command line for perago: queue:work, queue:listen,
// queue:retry, queue:failed, queue:monitor, make:job and the rest. Embed it in your app:
//
//	func main() {
//		app := &console.App{Manager: func(ctx context.Context) (*perago.Manager, error) { return buildManager(ctx) }}
//		os.Exit(app.Run(context.Background(), os.Args[1:]))
//	}
package console

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/LouisRiverstone/perago"
)

// Exit codes returned by Run (queue:work returns the worker's status).
const (
	ExitOK    = 0
	ExitError = 1
	ExitUsage = 2
)

// App wires the commands to your application. Zero values get defaults in Run.
type App struct {
	// Manager builds the queue manager on demand, so make:* commands never need one.
	Manager func(ctx context.Context) (*perago.Manager, error)

	// Env is the application environment ("production" asks before destructive commands).
	// Defaults to PERAGO_ENV, then APP_ENV, then "development".
	Env string

	// Directories, relative to Root, used by the make:* commands.
	Root          string
	JobsDir       string // app/jobs
	MiddlewareDir string // app/jobs/middleware
	MigrationsDir string // database/migrations

	// Executable and Args start a worker process for queue:listen; they default to this program.
	Executable string
	Args       []string

	// Maintenance reports maintenance mode to workers (see WorkerOptions.Maintenance).
	Maintenance func() bool

	Out, Err    io.Writer
	In          io.Reader
	Interactive bool // enables the production confirmation prompt
	Now         func() time.Time
}

func (a *App) defaults() {
	set := func(p *string, v string) {
		if *p == "" {
			*p = v
		}
	}
	set(&a.Root, ".")
	set(&a.JobsDir, "app/jobs")
	set(&a.MiddlewareDir, "app/jobs/middleware")
	set(&a.MigrationsDir, "database/migrations")
	for _, k := range []string{"PERAGO_ENV", "APP_ENV"} {
		set(&a.Env, os.Getenv(k))
	}
	set(&a.Env, "development")
	if a.Out == nil {
		a.Out = os.Stdout
	}
	if a.Err == nil {
		a.Err = os.Stderr
	}
	if a.In == nil {
		a.In = os.Stdin
	}
	if a.Now == nil {
		a.Now = time.Now
	}
}

type command struct {
	name    string
	usage   string
	summary string
	bools   []string
	run     func(c *call) int
}

type call struct {
	app   *App
	ctx   context.Context
	args  []string
	flags map[string]string
}

func (c *call) has(name string) bool { _, ok := c.flags[name]; return ok }

func (c *call) flag(name, fallback string) string {
	if v, ok := c.flags[name]; ok && v != "" {
		return v
	}
	return fallback
}

func (c *call) manager() (*perago.Manager, bool) {
	if c.app.Manager == nil {
		c.errorf("no queue manager configured (App.Manager)")
		return nil, false
	}
	m, err := c.app.Manager(c.ctx)
	if err != nil {
		c.errorf("%v", err)
		return nil, false
	}
	return m, true
}

func (c *call) printf(format string, args ...any) { _, _ = fmt.Fprintf(c.app.Out, format, args...) }

func (c *call) errorf(format string, args ...any) {
	_, _ = fmt.Fprintf(c.app.Err, "error: "+format+"\n", args...)
}

// confirm asks before destructive commands in production unless --force.
func (c *call) confirm(action string) bool {
	if c.app.Env != "production" || c.has("force") {
		return true
	}
	if !c.app.Interactive {
		c.errorf("refusing to %s in production without --force", action)
		return false
	}
	c.printf("Application in production. Do you really wish to %s? (yes/no) ", action)
	line, _ := bufio.NewReader(c.app.In).ReadString('\n')
	return strings.EqualFold(strings.TrimSpace(line), "yes")
}

// parse splits args into positional arguments and --flags (--name=value, --name value, --bool).
func parse(args []string, bools []string) ([]string, map[string]string, error) {
	isBool := map[string]bool{"force": true}
	for _, b := range bools {
		isBool[b] = true
	}
	flags := map[string]string{}
	var pos []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "--") {
			pos = append(pos, a)
			continue
		}
		name, value, hasValue := strings.Cut(strings.TrimPrefix(a, "--"), "=")
		if name == "" {
			return nil, nil, fmt.Errorf("invalid flag %q", a)
		}
		if !hasValue && !isBool[name] && i+1 < len(args) && !strings.HasPrefix(args[i+1], "--") {
			value = args[i+1]
			i++
		}
		if prev, ok := flags[name]; ok && prev != "" && value != "" {
			value = prev + "," + value
		}
		flags[name] = value
	}
	return pos, flags, nil
}

// Run executes a command line (without the program name) and returns the exit status.
func (a *App) Run(ctx context.Context, args []string) int {
	a.defaults()
	cmds := a.commands()
	if len(args) == 0 || args[0] == "list" || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		a.usage(cmds)
		return ExitOK
	}
	cmd, ok := cmds[args[0]]
	if !ok {
		_, _ = fmt.Fprintf(a.Err, "unknown command %q\n\n", args[0])
		a.usage(cmds)
		return ExitUsage
	}
	pos, flags, err := parse(args[1:], cmd.bools)
	if err != nil {
		_, _ = fmt.Fprintf(a.Err, "%v\nusage: %s\n", err, cmd.usage)
		return ExitUsage
	}
	if _, help := flags["help"]; help {
		_, _ = fmt.Fprintf(a.Out, "%s\n\n  %s\n", cmd.usage, cmd.summary)
		return ExitOK
	}
	return cmd.run(&call{app: a, ctx: ctx, args: pos, flags: flags})
}

func (a *App) usage(cmds map[string]command) {
	names := make([]string, 0, len(cmds))
	for n := range cmds {
		names = append(names, n)
	}
	sort.Strings(names)
	_, _ = fmt.Fprintln(a.Out, "perago: Laravel-style queues for Go\n\nCommands:")
	for _, n := range names {
		_, _ = fmt.Fprintf(a.Out, "  %-24s %s\n", n, cmds[n].summary)
	}
}

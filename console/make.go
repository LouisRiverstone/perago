package console

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"
	"unicode"

	"github.com/LouisRiverstone/perago/database"
)

var identifier = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)

func snake(name string) string {
	var b strings.Builder
	rs := []rune(name)
	for i, r := range rs {
		if unicode.IsUpper(r) && i > 0 && (unicode.IsLower(rs[i-1]) || (i+1 < len(rs) && unicode.IsLower(rs[i+1]))) {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}

func packageName(dir string) string {
	p := strings.ToLower(filepath.Base(filepath.Clean(dir)))
	p = strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return r
		}
		return -1
	}, p)
	if p == "" {
		return "jobs"
	}
	return p
}

// write renders a Go template into dir/file, refusing to overwrite.
func (c *call) write(dir, file string, tpl *template.Template, data any) int {
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, data); err != nil {
		c.errorf("%v", err)
		return ExitError
	}
	src, err := format.Source(buf.Bytes())
	if err != nil {
		c.errorf("generated code does not compile: %v", err)
		return ExitError
	}
	return c.save(dir, file, src)
}

// save writes src to dir/file, refusing to overwrite.
func (c *call) save(dir, file string, src []byte) int {
	full := filepath.Join(c.app.Root, dir)
	if err := os.MkdirAll(full, 0o755); err != nil {
		c.errorf("%v", err)
		return ExitError
	}
	path := filepath.Join(full, file)
	if _, err := os.Stat(path); err == nil {
		c.errorf("%s already exists", path)
		return ExitError
	}
	if err := os.WriteFile(path, src, 0o644); err != nil {
		c.errorf("%v", err)
		return ExitError
	}
	c.printf("Created %s\n", filepath.Join(dir, file))
	return ExitOK
}

func (c *call) typeName(kind string) (string, bool) {
	if len(c.args) != 1 || !identifier.MatchString(c.args[0]) {
		c.errorf("usage: make:%s <Name> (an exported Go identifier, e.g. ProcessPodcast)", kind)
		return "", false
	}
	return c.args[0], true
}

var jobTemplate = template.Must(template.New("job").Parse(`package {{.Package}}

import (
	"context"

	"github.com/LouisRiverstone/perago"
)

// {{.Name}} is a queued job. Its exported fields are its payload.
type {{.Name}} struct {
	perago.Queueable
{{- if .Sync}}
	perago.Sync // runs in the dispatching process
{{- end}}
}

func init() { perago.Register(&{{.Name}}{}) }

// Handle runs the job.
func (j *{{.Name}}) Handle(ctx context.Context) error {
{{- if .Batched}}
	if b, err := j.Batch(ctx); err != nil || (b != nil && b.Cancelled()) {
		return err
	}
{{- end}}
	return nil
}
`))

func makeJob(c *call) int {
	name, ok := c.typeName("job")
	if !ok {
		return ExitUsage
	}
	return c.write(c.app.JobsDir, snake(name)+".go", jobTemplate, map[string]any{
		"Package": packageName(c.app.JobsDir), "Name": name, "Sync": c.has("sync"), "Batched": c.has("batched"),
	})
}

var middlewareTemplate = template.Must(template.New("mw").Parse(`package {{.Package}}

import (
	"context"

	"github.com/LouisRiverstone/perago"
)

// {{.Name}} is a job middleware: list it in a job's Middleware method.
type {{.Name}} struct{}

// Handle runs before the job; call next to continue, or release/delete the job through
// perago.JobFrom(ctx) to stop.
func ({{.Name}}) Handle(ctx context.Context, job perago.Job, next perago.Next) error {
	return next(ctx)
}
`))

func makeMiddleware(c *call) int {
	name, ok := c.typeName("job-middleware")
	if !ok {
		return ExitUsage
	}
	return c.write(c.app.MiddlewareDir, snake(name)+".go", middlewareTemplate, map[string]any{
		"Package": packageName(c.app.MiddlewareDir), "Name": name,
	})
}

var migrationTemplate = template.Must(template.New("migration").Parse(`package {{.Package}}

import (
	"context"

	"github.com/LouisRiverstone/migro/migrate"
	"github.com/LouisRiverstone/migro/schema"
	peragomigro "github.com/LouisRiverstone/perago/migro"
)

type {{.Type}} struct{}

func init() { migrate.Register(&{{.Type}}{}) }

func ({{.Type}}) Up(ctx context.Context, s *schema.Builder) error {
	return peragomigro.{{.Func}}(ctx, s, {{printf "%q" .Table}})
}

func ({{.Type}}) Down(ctx context.Context, s *schema.Builder) error {
	return s.DropTableIfExists(ctx, {{printf "%q" .Table}})
}
`))

var tableName = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// usesMigro matches a go.mod that requires migro itself (not perago's migro module).
var usesMigro = regexp.MustCompile(`(?m)^\s*(require\s+)?github\.com/LouisRiverstone/migro\s+v`)

// tableFormat is "migro" on a project that requires migro, "sql" anywhere else.
func (c *call) tableFormat() string {
	mod, err := os.ReadFile(filepath.Join(c.app.Root, "go.mod"))
	if err == nil && usesMigro.Match(mod) {
		return "migro"
	}
	return "sql"
}

// makeTable writes a migration creating one of perago's tables: a migro migration on a migro
// project (or with --format=migro), otherwise a .sql file for --dialect (postgres by default).
func makeTable(defaultTable, fn string) func(c *call) int {
	return func(c *call) int {
		table := c.flag("table", defaultTable)
		if !tableName.MatchString(table) {
			c.errorf("--table must be snake_case, got %q", table)
			return ExitUsage
		}
		stem := fmt.Sprintf("%s_create_%s_table", c.app.Now().UTC().Format("20060102150405"), table)
		switch c.flag("format", c.tableFormat()) {
		case "migro":
			return c.write(c.app.MigrationsDir, stem+".go", migrationTemplate, map[string]any{
				"Package": packageName(c.app.MigrationsDir), "Type": "Create" + camel(table) + "Table", "Func": fn, "Table": table,
			})
		case "sql":
			return c.sqlMigration(defaultTable, table, stem+".sql")
		}
		c.errorf("--format must be migro or sql")
		return ExitUsage
	}
}

func (c *call) sqlMigration(kind, table, file string) int {
	dialect, err := database.ParseDialect(c.flag("dialect", "postgres"))
	if err != nil {
		c.errorf("%v", err)
		return ExitUsage
	}
	stmts, err := database.TableSQL(dialect, kind, table)
	if err != nil {
		c.errorf("%v", err)
		return ExitError
	}
	src := fmt.Sprintf("-- perago %s table for %s, generated by perago.\n\n%s;\n", table, dialect, strings.Join(stmts, ";\n\n"))
	return c.save(c.app.MigrationsDir, file, []byte(src))
}

func camel(s string) string {
	parts := strings.Split(s, "_")
	for i, p := range parts {
		if p != "" {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, "")
}

package database

import (
	"context"
	"database/sql"
	"strconv"
	"strings"

	"github.com/LouisRiverstone/perago"
)

// Dialect names the SQL dialect the statements are written for.
type Dialect string

// The dialects perago speaks, the same set migro supports.
const (
	Postgres    Dialect = "postgres"
	CockroachDB Dialect = "cockroachdb"
	Redshift    Dialect = "redshift"
	MySQL       Dialect = "mysql"
	MariaDB     Dialect = "mariadb"
	SQLite      Dialect = "sqlite"
	SQLServer   Dialect = "sqlserver"
	Oracle      Dialect = "oracle"
)

var aliases = map[string]Dialect{
	"postgres": Postgres, "postgresql": Postgres, "pg": Postgres, "pgx": Postgres,
	"cockroachdb": CockroachDB, "cockroach": CockroachDB,
	"redshift": Redshift,
	"mysql":    MySQL,
	"mariadb":  MariaDB,
	"sqlite":   SQLite, "sqlite3": SQLite,
	"sqlserver": SQLServer, "mssql": SQLServer,
	"oracle": Oracle, "oracledb": Oracle,
}

// ParseDialect resolves a dialect name or one of its aliases (pg, pgx, sqlite3, mssql, ...).
func ParseDialect(name string) (Dialect, error) {
	d, ok := aliases[strings.ToLower(strings.TrimSpace(name))]
	if !ok {
		return "", perago.Newf(perago.InvalidConfig, "database: unknown dialect %q", name)
	}
	return d, nil
}

func (d Dialect) postgresFamily() bool { return d == Postgres || d == CockroachDB || d == Redshift }

func (d Dialect) mysqlFamily() bool { return d == MySQL || d == MariaDB }

// Quote quotes an identifier the way the dialect (and migro) does.
func (d Dialect) Quote(ident string) string {
	switch {
	case d.mysqlFamily():
		return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
	case d == SQLServer:
		return "[" + strings.ReplaceAll(ident, "]", "]]") + "]"
	case d == Oracle:
		ident = strings.ToUpper(ident)
	}
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

// rebind turns the ? placeholders into the dialect's own.
func (d Dialect) rebind(query string) string {
	var prefix string
	switch {
	case d.postgresFamily():
		prefix = "$"
	case d == SQLServer:
		prefix = "@p"
	case d == Oracle:
		prefix = ":"
	default:
		return query
	}
	var b strings.Builder
	n := 0
	for _, r := range query {
		if r != '?' {
			b.WriteRune(r)
			continue
		}
		n++
		b.WriteString(prefix + strconv.Itoa(n))
	}
	return b.String()
}

// Conn runs statements: *sql.DB, *sql.Tx, *sql.Conn and gorm's ConnPool all satisfy it.
type Conn interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type beginner interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// DB is a connection and the dialect perago writes its statements in. It needs nothing but
// database/sql: open the connection with any driver (pgx, go-sql-driver/mysql, modernc sqlite,
// go-mssqldb, go-ora) or hand over one from an ORM.
type DB struct {
	conn    Conn
	dialect Dialect
}

// New wraps conn for the stores and the driver. dialect is a Dialect or an alias of one.
func New(conn Conn, dialect string) (*DB, error) {
	if conn == nil {
		return nil, perago.Newf(perago.InvalidConfig, "database: nil connection")
	}
	d, err := ParseDialect(dialect)
	if err != nil {
		return nil, err
	}
	return &DB{conn: conn, dialect: d}, nil
}

// Dialect returns the dialect statements are written for.
func (db *DB) Dialect() Dialect { return db.dialect }

func (db *DB) exec(ctx context.Context, code perago.Code, query string, args ...any) (int64, error) {
	res, err := db.conn.ExecContext(ctx, db.dialect.rebind(query), args...)
	if err != nil {
		return 0, perago.Wrap(code, err, "database")
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, perago.Wrap(code, err, "database")
	}
	return n, nil
}

func (db *DB) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return db.conn.QueryContext(ctx, db.dialect.rebind(query), args...)
}

func (db *DB) queryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return db.conn.QueryRowContext(ctx, db.dialect.rebind(query), args...)
}

// transaction runs fn in a transaction, or straight on the connection when it already is one.
func (db *DB) transaction(ctx context.Context, fn func(*DB) error) error {
	b, ok := db.conn.(beginner)
	if !ok {
		return fn(db)
	}
	tx, err := b.BeginTx(ctx, nil)
	if err != nil {
		return perago.Wrap(perago.DriverFailed, err, "begin transaction")
	}
	if err := fn(&DB{conn: tx, dialect: db.dialect}); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return perago.Wrap(perago.DriverFailed, err, "commit transaction")
	}
	return nil
}

// sqlBuilder renders the few statements the stores need, quoted for the dialect.
type sqlBuilder struct {
	dialect Dialect
	table   string
}

func (b sqlBuilder) q(ident string) string { return b.dialect.Quote(ident) }

func (b sqlBuilder) t() string { return b.q(b.table) }

func (b sqlBuilder) cols(names ...string) string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = b.q(n)
	}
	return strings.Join(out, ", ")
}

// limit wraps "SELECT <rest>" so it returns at most n rows.
func (b sqlBuilder) limit(rest string, n int) string {
	switch b.dialect {
	case SQLServer:
		return "SELECT TOP (" + strconv.Itoa(n) + ") " + rest
	case Oracle:
		return "SELECT " + rest + " FETCH FIRST " + strconv.Itoa(n) + " ROWS ONLY"
	}
	return "SELECT " + rest + " LIMIT " + strconv.Itoa(n)
}

func (b sqlBuilder) insert(cols ...string) string {
	marks := strings.TrimSuffix(strings.Repeat("?, ", len(cols)), ", ")
	return "INSERT INTO " + b.t() + " (" + b.cols(cols...) + ") VALUES (" + marks + ")"
}

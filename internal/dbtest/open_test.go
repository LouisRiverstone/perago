package dbtest_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/sijms/go-ora/v2"
	_ "modernc.org/sqlite"

	"github.com/LouisRiverstone/perago/database"
)

// drivers maps each dialect to the database/sql driver the tests open it with.
var drivers = map[database.Dialect]string{
	database.Postgres: "pgx", database.CockroachDB: "pgx", database.Redshift: "pgx",
	database.MySQL: "mysql", database.MariaDB: "mysql",
	database.SQLite:    "sqlite",
	database.SQLServer: "sqlserver",
	database.Oracle:    "oracle",
}

// open returns the database the tests run against, SQLite in a temp dir by default or
// PERAGO_TEST_DIALECT / PERAGO_TEST_DSN, and drops the given tables before and after the test.
func open(t *testing.T, tables ...string) (*database.DB, *sql.DB) {
	t.Helper()
	name, dsn := os.Getenv("PERAGO_TEST_DIALECT"), os.Getenv("PERAGO_TEST_DSN")
	if name == "" {
		name = "sqlite"
		dsn = "file:" + filepath.Join(t.TempDir(), "perago.db") + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)"
	}
	dialect, err := database.ParseDialect(name)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := sql.Open(drivers[dialect], dsn)
	if err != nil {
		t.Fatal(err)
	}
	drop := func() {
		for _, tb := range tables {
			_, _ = sqlDB.Exec("DROP TABLE " + dialect.Quote(tb))
		}
	}
	drop()
	t.Cleanup(func() {
		drop()
		_ = sqlDB.Close()
	})
	db, err := database.New(sqlDB, name)
	if err != nil {
		t.Fatal(err)
	}
	return db, sqlDB
}

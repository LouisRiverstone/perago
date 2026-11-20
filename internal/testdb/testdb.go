// Package testdb opens the database the tests run against: SQLite in a temp dir by default, or
// PERAGO_TEST_DIALECT / PERAGO_TEST_DSN (used by CI for every migro dialect).
package testdb

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/LouisRiverstone/migro"
	_ "github.com/LouisRiverstone/migro/dialect/mysql"
	_ "github.com/LouisRiverstone/migro/dialect/oracle"
	_ "github.com/LouisRiverstone/migro/dialect/postgres"
	_ "github.com/LouisRiverstone/migro/dialect/sqlite"
	_ "github.com/LouisRiverstone/migro/dialect/sqlserver"
	"github.com/LouisRiverstone/migro/schema"
)

// Open returns a database and drops the given tables before and after the test.
func Open(t *testing.T, tables ...string) *migro.DB {
	t.Helper()
	dialect, dsn := os.Getenv("PERAGO_TEST_DIALECT"), os.Getenv("PERAGO_TEST_DSN")
	if dialect == "" {
		dialect = "sqlite"
		dsn = "file:" + filepath.Join(t.TempDir(), "perago.db") + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)"
	}
	db, err := migro.Open(migro.Config{Dialect: dialect, DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	drop := func() {
		s := schema.New(db)
		for _, tb := range tables {
			_ = s.DropTableIfExists(context.Background(), tb)
		}
	}
	drop()
	t.Cleanup(func() {
		drop()
		_ = db.Close()
	})
	return db
}

// Dialect is the dialect the tests run on.
func Dialect() string {
	if d := os.Getenv("PERAGO_TEST_DIALECT"); d != "" {
		return d
	}
	return "sqlite"
}

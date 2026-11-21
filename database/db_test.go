package database

import (
	"strings"
	"testing"
)

func TestParseDialect(t *testing.T) {
	for in, want := range map[string]Dialect{"pgx": Postgres, "Cockroach": CockroachDB, "sqlite3": SQLite, "mssql": SQLServer, "oracledb": Oracle, "mariadb": MariaDB} {
		if got, err := ParseDialect(in); err != nil || got != want {
			t.Errorf("ParseDialect(%q) = %q, %v", in, got, err)
		}
	}
	if _, err := ParseDialect("db2"); err == nil {
		t.Error("db2 accepted")
	}
	if _, err := New(nil, "postgres"); err == nil {
		t.Error("nil connection accepted")
	}
}

func TestQuoteAndRebind(t *testing.T) {
	cases := []struct {
		d            Dialect
		quoted, stmt string
	}{
		{Postgres, `"jobs"`, "a = $1 AND b = $2"},
		{MySQL, "`jobs`", "a = ? AND b = ?"},
		{SQLite, `"jobs"`, "a = ? AND b = ?"},
		{SQLServer, "[jobs]", "a = @p1 AND b = @p2"},
		{Oracle, `"JOBS"`, "a = :1 AND b = :2"},
	}
	for _, c := range cases {
		if got := c.d.Quote("jobs"); got != c.quoted {
			t.Errorf("%s quote = %s", c.d, got)
		}
		if got := c.d.rebind("a = ? AND b = ?"); got != c.stmt {
			t.Errorf("%s rebind = %s", c.d, got)
		}
	}
}

func TestTableSQL(t *testing.T) {
	for _, d := range []Dialect{Postgres, CockroachDB, Redshift, MySQL, MariaDB, SQLite, SQLServer, Oracle} {
		for _, table := range []string{JobsTable, FailedTable, BatchesTable, CacheTable} {
			stmts, err := TableSQL(d, table, "")
			if err != nil || len(stmts) == 0 || !strings.HasPrefix(stmts[0], "CREATE TABLE "+d.Quote(table)+" (") {
				t.Fatalf("%s %s = %v %v", d, table, stmts, err)
			}
			if d == Redshift && len(stmts) != 1 {
				t.Errorf("redshift got indexes: %v", stmts)
			}
		}
	}
	jobs, _ := TableSQL(MySQL, JobsTable, "queue_jobs")
	if !strings.Contains(jobs[0], "`payload` longtext NOT NULL") || !strings.Contains(jobs[0], "auto_increment") {
		t.Errorf("mysql jobs = %s", jobs[0])
	}
	if !strings.Contains(jobs[2], "`queue_jobs_queue_reserved_at_available_at_index`") {
		t.Errorf("mysql index = %s", jobs[2])
	}
	ora, _ := TableSQL(Oracle, JobsTable, "")
	if !strings.Contains(ora[0], `"ATTEMPTS" NUMBER(10) DEFAULT 0 NOT NULL`) {
		t.Errorf("oracle jobs = %s", ora[0])
	}
	if _, err := TableSQL(Postgres, "users", ""); err == nil {
		t.Error("unknown table accepted")
	}
	if _, err := TableSQL("db2", JobsTable, ""); err == nil {
		t.Error("unknown dialect accepted")
	}
	if n := indexName(strings.Repeat("t", 80), []string{"a"}); len(n) != maxIdentLen {
		t.Errorf("long index name %q", n)
	}
}

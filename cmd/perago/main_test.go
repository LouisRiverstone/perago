package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMakeRunsWithoutApp(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/app\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	if code := run([]string{"make:job", "ImportCsv"}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if _, err := os.Stat(filepath.Join(dir, "app/jobs/import_csv.go")); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"queue:work"}); code != 2 {
		t.Fatalf("queue:work without an app console: exit %d", code)
	}
	if code := run([]string{"--version"}); code != 0 {
		t.Fatal("version")
	}
}

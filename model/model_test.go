package model_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/LouisRiverstone/migro/orm"
	"github.com/LouisRiverstone/migro/schema"
	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/internal/testdb"
	"github.com/LouisRiverstone/perago/model"
)

var ctx = context.Background()

type Customer struct {
	orm.BaseModel
	Name string
}

type Greet struct {
	perago.DeletesMissingModels
	Customer model.Of[Customer]
	Others   model.Many[Customer]
}

var greeted []string

func (g *Greet) Handle(context.Context) error {
	greeted = append(greeted, g.Customer.Get().Name)
	for _, o := range g.Others.Get() {
		greeted = append(greeted, o.Name)
	}
	return nil
}

func init() { perago.Register(&Greet{}) }

func TestSerializesModels(t *testing.T) {
	db := testdb.Open(t, "customers")
	err := schema.New(db).CreateTable(ctx, "customers", func(tb *schema.Table) {
		tb.BigIncrements("id")
		tb.String("name")
		tb.Timestamps()
	})
	if err != nil {
		t.Fatal(err)
	}
	model.Use(db)
	customers := orm.New[Customer](db)
	ana := &Customer{Name: "Ana"}
	bob := &Customer{Name: "Bob"}
	for _, c := range []*Customer{ana, bob} {
		if err := customers.Create(ctx, c); err != nil {
			t.Fatal(err)
		}
	}

	q := perago.NewMemoryDriver(0)
	failed := perago.NewMemoryFailedJobStore()
	m, _ := perago.New(perago.Config{Default: "mem", Connections: map[string]perago.Connection{"mem": {Driver: q}}, Failed: failed})
	job := &Greet{Customer: model.New(ana), Others: model.NewMany([]Customer{*bob})}
	if _, err := m.Dispatch(job).Send(ctx); err != nil {
		t.Fatal(err)
	}
	raw := string(q.Payloads("default")[0])
	if strings.Contains(raw, "Ana") {
		t.Fatalf("model attributes leaked into the payload: %s", raw)
	}
	// Rename before the job runs: the worker sees the fresh row.
	ana.Name = "Ana Maria"
	if err := customers.Save(ctx, ana); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Work(ctx, perago.WorkerOptions{StopWhenEmpty: true}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(greeted, ",") != "Ana Maria,Bob" {
		t.Fatalf("greeted = %v", greeted)
	}

	// The model is gone: DeleteWhenMissingModels drops the job quietly.
	greeted = nil
	_, _ = m.Dispatch(&Greet{Customer: model.New(bob)}).Send(ctx)
	if err := customers.Delete(ctx, bob); err != nil {
		t.Fatal(err)
	}
	_, _ = m.Work(ctx, perago.WorkerOptions{StopWhenEmpty: true})
	if len(greeted) != 0 {
		t.Fatal("job ran without its model")
	}
	if all, _ := failed.All(ctx); len(all) != 0 {
		t.Fatalf("missing model was stored as a failure: %v", all)
	}
	if n, _ := q.Size(ctx, "default"); n != 0 {
		t.Fatal("job not deleted")
	}
}

func TestKeyRoundTrip(t *testing.T) {
	ref := model.Key[Customer](42).With("Orders")
	b, _ := json.Marshal(ref)
	if string(b) != `{"key":42,"with":["Orders"]}` {
		t.Fatal(string(b))
	}
	var back model.Of[Customer]
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.Get() != nil {
		t.Fatal("value should be empty until Restore")
	}
}

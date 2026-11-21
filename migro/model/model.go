// Package model is Laravel's SerializesModels for migro models: a job field of type model.Of[T]
// travels as the model's primary key and is loaded again, fresh, right before the job runs.
// Relations are never serialized (Laravel's withoutRelations); list the ones to eager load
// again with With.
//
//	type SendInvoice struct {
//		perago.DeletesMissingModels           // delete the job if the order is gone
//		Order model.Of[Order]
//	}
//
//	m.Dispatch(&SendInvoice{Order: model.New(order).With("Lines")}).Send(ctx)
//
//	func (j *SendInvoice) Handle(ctx context.Context) error {
//		order := j.Order.Get()                // *Order, loaded by the worker
//		...
//	}
package model

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"

	"github.com/LouisRiverstone/migro"
	"github.com/LouisRiverstone/migro/orm"
	"github.com/LouisRiverstone/perago"
	"gorm.io/gorm/schema"
)

var conns = struct {
	sync.RWMutex
	def   *migro.DB
	named map[string]*migro.DB
}{named: map[string]*migro.DB{}}

// Use sets the database models are loaded from.
func Use(db *migro.DB) {
	conns.Lock()
	defer conns.Unlock()
	conns.def = db
}

// UseNamed registers a named database, chosen per model with On.
func UseNamed(name string, db *migro.DB) {
	conns.Lock()
	defer conns.Unlock()
	conns.named[name] = db
}

func database(name string) (*migro.DB, error) {
	conns.RLock()
	defer conns.RUnlock()
	if name == "" {
		if conns.def == nil {
			return nil, perago.Newf(perago.InvalidConfig, "model: no database; call model.Use(db) at startup")
		}
		return conns.def, nil
	}
	db, ok := conns.named[name]
	if !ok {
		return nil, perago.Newf(perago.InvalidConfig, "model: database %q is not registered (model.UseNamed)", name)
	}
	return db, nil
}

var schemas sync.Map

func primaryField[T any]() (*schema.Field, error) {
	sch, err := schema.Parse(new(T), &schemas, schema.NamingStrategy{})
	if err != nil {
		return nil, perago.Wrap(perago.InvalidConfig, err, "model: parse %T", *new(T))
	}
	if sch.PrioritizedPrimaryField == nil {
		return nil, perago.Newf(perago.InvalidConfig, "model: %T has no primary key", *new(T))
	}
	return sch.PrioritizedPrimaryField, nil
}

// Of is a model reference that serializes as its primary key.
type Of[T any] struct {
	key        json.RawMessage
	with       []string
	connection string
	value      *T
}

type wire struct {
	Key        json.RawMessage `json:"key"`
	With       []string        `json:"with,omitempty"`
	Connection string          `json:"connection,omitempty"`
}

// New references v by its primary key.
func New[T any](v *T) Of[T] {
	o := Of[T]{value: v}
	f, err := primaryField[T]()
	if err != nil || v == nil {
		return o
	}
	key, _ := f.ValueOf(context.Background(), reflect.ValueOf(v).Elem())
	o.key, _ = json.Marshal(key)
	return o
}

// Key references a model by primary key without loading it.
func Key[T any](key any) Of[T] {
	raw, _ := json.Marshal(key)
	return Of[T]{key: raw}
}

// With eager loads relations when the model is restored.
func (o Of[T]) With(relations ...string) Of[T] {
	o.with = append(append([]string(nil), o.with...), relations...)
	return o
}

// On loads the model from a database registered with UseNamed.
func (o Of[T]) On(connection string) Of[T] { o.connection = connection; return o }

// Get is the model: the one given to New before dispatch, the fresh copy inside the job.
func (o Of[T]) Get() *T { return o.value }

func (o Of[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(wire{Key: o.key, With: o.with, Connection: o.connection})
}

func (o *Of[T]) UnmarshalJSON(b []byte) error {
	var w wire
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	o.key, o.with, o.connection, o.value = w.Key, w.With, w.Connection, nil
	return nil
}

// typedKey decodes a JSON key into the primary key's Go type.
func typedKey(f *schema.Field, raw json.RawMessage) (any, error) {
	ptr := reflect.New(f.FieldType)
	if err := json.Unmarshal(raw, ptr.Interface()); err != nil {
		return nil, perago.Wrap(perago.PayloadInvalid, err, "model key")
	}
	return ptr.Elem().Interface(), nil
}

func notFound(err error, what string) error {
	if errors.Is(err, migro.ErrNotFound) {
		return perago.Wrap(perago.ModelNotFound, err, "%s", what)
	}
	return err
}

// Restore loads the model again (called by the worker before Handle).
func (o *Of[T]) Restore(ctx context.Context) error {
	if len(o.key) == 0 || string(o.key) == "null" {
		return nil
	}
	db, err := database(o.connection)
	if err != nil {
		return err
	}
	f, err := primaryField[T]()
	if err != nil {
		return err
	}
	key, err := typedKey(f, o.key)
	if err != nil {
		return err
	}
	q := orm.New[T](db).Query()
	for _, r := range o.with {
		q = q.With(r)
	}
	v, err := q.Where(db.Dialect.Quote(f.DBName)+" = ?", key).First(ctx)
	if err != nil {
		return notFound(err, "model "+string(o.key))
	}
	o.value = v
	return nil
}

// Many is a collection of model references (Laravel's serialized Eloquent collections).
type Many[T any] struct {
	keys       []json.RawMessage
	with       []string
	connection string
	values     []T
}

// NewMany references every model of vs by primary key.
func NewMany[T any](vs []T) Many[T] {
	m := Many[T]{values: vs}
	f, err := primaryField[T]()
	if err != nil {
		return m
	}
	for i := range vs {
		key, _ := f.ValueOf(context.Background(), reflect.ValueOf(&vs[i]).Elem())
		raw, _ := json.Marshal(key)
		m.keys = append(m.keys, raw)
	}
	return m
}

func (m Many[T]) With(relations ...string) Many[T] {
	m.with = append(append([]string(nil), m.with...), relations...)
	return m
}

func (m Many[T]) On(connection string) Many[T] { m.connection = connection; return m }

// Get is the models, in the order they were referenced.
func (m Many[T]) Get() []T { return m.values }

func (m Many[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Keys       []json.RawMessage `json:"keys"`
		With       []string          `json:"with,omitempty"`
		Connection string            `json:"connection,omitempty"`
	}{m.keys, m.with, m.connection})
}

func (m *Many[T]) UnmarshalJSON(b []byte) error {
	var w struct {
		Keys       []json.RawMessage `json:"keys"`
		With       []string          `json:"with"`
		Connection string            `json:"connection"`
	}
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	m.keys, m.with, m.connection, m.values = w.Keys, w.With, w.Connection, nil
	return nil
}

// Restore loads every model; a missing one is a MODEL_NOT_FOUND error.
func (m *Many[T]) Restore(ctx context.Context) error {
	if len(m.keys) == 0 {
		return nil
	}
	db, err := database(m.connection)
	if err != nil {
		return err
	}
	f, err := primaryField[T]()
	if err != nil {
		return err
	}
	keys := make([]any, 0, len(m.keys))
	for _, raw := range m.keys {
		k, err := typedKey(f, raw)
		if err != nil {
			return err
		}
		keys = append(keys, k)
	}
	q := orm.New[T](db).Query()
	for _, r := range m.with {
		q = q.With(r)
	}
	found, err := q.WhereIn(f.DBName, keys).Get(ctx)
	if err != nil {
		return err
	}
	byKey := map[any]T{}
	for _, v := range found {
		k, _ := f.ValueOf(ctx, reflect.ValueOf(&v).Elem())
		byKey[k] = v
	}
	out := make([]T, 0, len(keys))
	for i, k := range keys {
		v, ok := byKey[k]
		if !ok {
			return perago.Newf(perago.ModelNotFound, "model %s not found", string(m.keys[i]))
		}
		out = append(out, v)
	}
	m.values = out
	return nil
}

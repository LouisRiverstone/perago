package perago

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"sync"
	"time"
)

// Job is a unit of background work. Its exported fields are its payload: they are JSON-encoded
// on dispatch and decoded into a fresh value, built by the registered factory, before Handle runs.
//
// A job opts into Laravel's per-job settings by declaring the matching method:
//
//	Tries() int                              // #[Tries] / $tries; 0 retries forever
//	Backoff() []time.Duration                // #[Backoff] / backoff(); the last value repeats
//	Timeout() time.Duration                  // #[Timeout] / $timeout
//	RetryUntil() time.Time                   // retryUntil(); wins over Tries
//	RetryFor() time.Duration                 // #[RetryFor]: RetryUntil relative to the dispatch
//	MaxExceptions() int                      // #[MaxExceptions]
//	FailOnTimeout() bool                     // #[FailOnTimeout] (or embed FailsOnTimeout)
//	CountCrashesAsExceptions() bool          // #[CountCrashesAsExceptions] (or embed CountsCrashes)
//	Failed(ctx context.Context, err error)   // failed(): runs once when the job fails for good
//	Middleware() []Middleware                // middleware()
//	UniqueID() string                        // with ShouldBeUnique
//	UniqueFor() time.Duration                // #[UniqueFor]
//	UniqueVia() Cache                        // uniqueVia()
//	DebounceFor() time.Duration              // #[DebounceFor]
//	DebounceMaxWait() time.Duration          // #[DebounceFor(maxWait:)]
//	DebounceID() string                      // debounceId()
//	DebounceVia() Cache                      // debounceVia()
//	PrepareForDispatch(ctx context.Context) bool // PreparesForDispatch; false cancels the dispatch
//	DeleteWhenMissingModels() bool           // #[DeleteWhenMissingModels] (or embed DeletesMissingModels)
//	DisplayName() string                     // displayName()
//	MessageGroup() string                    // SQS FIFO / fair queues
//	DeduplicationID() string                 // SQS FIFO
//	JobName() string                         // the registered name; defaults to package.Type
//
// and the marker interfaces ShouldBeUnique, ShouldBeUniqueUntilProcessing, ShouldBeEncrypted,
// ShouldQueueAfterCommit and ShouldNotQueue, satisfied by embedding Unique, UniqueUntilProcessing,
// Encrypted, QueueAfterCommit and Sync.
type Job interface {
	Handle(ctx context.Context) error
}

// Optional job behaviors, detected with type assertions.
type (
	withTries         interface{ Tries() int }
	withBackoff       interface{ Backoff() []time.Duration }
	withTimeout       interface{ Timeout() time.Duration }
	withRetryUntil    interface{ RetryUntil() time.Time }
	withRetryFor      interface{ RetryFor() time.Duration }
	withMaxExceptions interface{ MaxExceptions() int }
	withFailOnTimeout interface{ FailOnTimeout() bool }
	withCountCrashes  interface{ CountCrashesAsExceptions() bool }
	withFailed        interface {
		Failed(ctx context.Context, err error)
	}
	withMiddleware      interface{ Middleware() []Middleware }
	withUniqueID        interface{ UniqueID() string }
	withUniqueFor       interface{ UniqueFor() time.Duration }
	withUniqueVia       interface{ UniqueVia() Cache }
	withDebounceFor     interface{ DebounceFor() time.Duration }
	withDebounceMaxWait interface{ DebounceMaxWait() time.Duration }
	withDebounceID      interface{ DebounceID() string }
	withDebounceVia     interface{ DebounceVia() Cache }
	withPrepare         interface {
		PrepareForDispatch(ctx context.Context) bool
	}
	withDeleteMissing   interface{ DeleteWhenMissingModels() bool }
	withDisplayName     interface{ DisplayName() string }
	withMessageGroup    interface{ MessageGroup() string }
	withDeduplicationID interface{ DeduplicationID() string }
	withJobName         interface{ JobName() string }
)

// Marker interfaces (Laravel's ShouldBeUnique, ShouldBeEncrypted, ...).
type (
	ShouldBeUnique                interface{ ShouldBeUnique() }
	ShouldBeUniqueUntilProcessing interface{ ShouldBeUniqueUntilProcessing() }
	ShouldBeEncrypted             interface{ ShouldBeEncrypted() }
	ShouldQueueAfterCommit        interface{ ShouldQueueAfterCommit() }
	// ShouldNotQueue jobs run in the dispatching process (a job made with make:job --sync).
	ShouldNotQueue interface{ ShouldNotQueue() }
)

// Embeddable markers. They have no fields, so they never show up in the payload.
type (
	Unique                struct{}
	UniqueUntilProcessing struct{}
	Encrypted             struct{}
	QueueAfterCommit      struct{}
	Sync                  struct{}
	FailsOnTimeout        struct{}
	CountsCrashes         struct{}
	DeletesMissingModels  struct{}
)

func (Unique) ShouldBeUnique()                               {}
func (UniqueUntilProcessing) ShouldBeUnique()                {}
func (UniqueUntilProcessing) ShouldBeUniqueUntilProcessing() {}
func (Encrypted) ShouldBeEncrypted()                         {}
func (QueueAfterCommit) ShouldQueueAfterCommit()             {}
func (Sync) ShouldNotQueue()                                 {}
func (FailsOnTimeout) FailOnTimeout() bool                   { return true }
func (CountsCrashes) CountCrashesAsExceptions() bool         { return true }
func (DeletesMissingModels) DeleteWhenMissingModels() bool   { return true }

// Restorable is implemented by payload fields that must be reloaded before the job runs,
// such as model.Of[T]: only the key travels, the record is fetched again in the worker.
// Returning an error matching ErrModelNotFound deletes the job when it DeleteWhenMissingModels.
type Restorable interface {
	Restore(ctx context.Context) error
}

// Factory builds an empty job ready for json.Unmarshal; use it to inject dependencies
// that are not part of the payload (fields tagged `json:"-"`).
type Factory func() Job

var registry = struct {
	sync.RWMutex
	byName map[string]Factory
	byType map[reflect.Type]string
}{byName: map[string]Factory{}, byType: map[reflect.Type]string{}}

// Register makes a job type known to the workers under NameOf(prototype). Call it from init().
//
//	func init() { perago.Register(&ProcessPodcast{}) }
func Register(prototype Job) { RegisterAs(NameOf(prototype), prototype) }

// RegisterAs registers a job under an explicit name, for example to keep old payloads working
// after a rename.
func RegisterAs(name string, prototype Job) {
	t := baseType(prototype)
	RegisterFactory(name, func() Job { return reflect.New(t).Interface().(Job) })
	registry.Lock()
	defer registry.Unlock()
	if _, ok := registry.byType[t]; !ok {
		registry.byType[t] = name
	}
}

// RegisterFactory registers a job built by factory, so the worker can hand it dependencies:
//
//	perago.RegisterFactory("emails.Send", func() perago.Job { return &SendEmail{Mailer: mailer} })
//
// Registering the same name twice replaces the factory.
func RegisterFactory(name string, factory Factory) {
	registry.Lock()
	defer registry.Unlock()
	registry.byName[name] = factory
	j := factory()
	if j == nil {
		return
	}
	t := baseType(j)
	if _, ok := registry.byType[t]; !ok {
		registry.byType[t] = name
	}
}

// Registered lists every registered job name, sorted.
func Registered() []string {
	registry.RLock()
	defer registry.RUnlock()
	out := make([]string, 0, len(registry.byName))
	for n := range registry.byName {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func lookupFactory(name string) (Factory, bool) {
	registry.RLock()
	defer registry.RUnlock()
	f, ok := registry.byName[name]
	return f, ok
}

func baseType(v any) reflect.Type {
	t := reflect.TypeOf(v)
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

// NameOf is the name a job travels under: JobName() when declared, the registered name for its
// type, or "package.Type".
func NameOf(job any) string {
	if n, ok := job.(withJobName); ok {
		return n.JobName()
	}
	t := baseType(job)
	if t == nil {
		return "<nil>"
	}
	registry.RLock()
	name, ok := registry.byType[t]
	registry.RUnlock()
	if ok {
		return name
	}
	return typeName(t)
}

func typeName(t reflect.Type) string {
	if t.Name() == "" {
		return t.String()
	}
	pkg := t.PkgPath()
	for i := len(pkg) - 1; i >= 0; i-- {
		if pkg[i] == '/' {
			pkg = pkg[i+1:]
			break
		}
	}
	if pkg == "" {
		return t.Name()
	}
	return pkg + "." + t.Name()
}

// DisplayNameOf is DisplayName() when declared, else NameOf.
func DisplayNameOf(job any) string {
	if d, ok := job.(withDisplayName); ok {
		return d.DisplayName()
	}
	return NameOf(job)
}

// Closure wraps a function as a job (Laravel's dispatch(function () {...})).
// A Go closure cannot be serialized, so it only runs on the in-process connections (sync,
// deferred, background) of the Manager that dispatched it. Use RegisterClosure and Call for
// closures that must cross process boundaries.
func Closure(fn func(ctx context.Context) error) Job { return &closureJob{fn: fn} }

type closureJob struct {
	ID string `json:"id"`
	fn func(ctx context.Context) error
}

func (c *closureJob) Handle(ctx context.Context) error {
	if c.fn == nil {
		return Newf(ClosureNotSerializable, "closure %s is not available in this process", c.ID)
	}
	return c.fn(ctx)
}

func (*closureJob) JobName() string     { return closureName }
func (*closureJob) DisplayName() string { return "Closure" }

const (
	closureName = "perago.Closure"
	callName    = "perago.Call"
)

var closures = struct {
	sync.RWMutex
	m map[string]func(ctx context.Context, args json.RawMessage) error
}{m: map[string]func(ctx context.Context, args json.RawMessage) error{}}

// RegisterClosure registers a named function that any worker can run with Call.
func RegisterClosure(name string, fn func(ctx context.Context, args json.RawMessage) error) {
	closures.Lock()
	defer closures.Unlock()
	closures.m[name] = fn
}

// Call is a serializable closure job: it runs the function registered under name with args
// encoded as JSON.
func Call(name string, args any) Job {
	raw, err := json.Marshal(args)
	if err != nil {
		raw = nil
	}
	return &callJob{Name: name, Args: raw}
}

type callJob struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"args,omitempty"`
}

func (c *callJob) Handle(ctx context.Context) error {
	closures.RLock()
	fn, ok := closures.m[c.Name]
	closures.RUnlock()
	if !ok {
		return Newf(JobNotRegistered, "closure %q is not registered (perago.RegisterClosure)", c.Name)
	}
	return fn(ctx, c.Args)
}

func (*callJob) JobName() string       { return callName }
func (c *callJob) DisplayName() string { return "Closure (" + c.Name + ")" }

func init() {
	RegisterFactory(callName, func() Job { return &callJob{} })
}

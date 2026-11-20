package dynamodb_test

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/dynamodb"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsdb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var ctx = context.Background()

type item = map[string]types.AttributeValue

// fakeDB understands the key schema (application + one sort key) and the update expressions the
// stores send.
type fakeDB struct {
	mu    sync.Mutex
	items map[string]item
}

func newFake() *fakeDB { return &fakeDB{items: map[string]item{}} }

func sv(it item, k string) string {
	if v, ok := it[k].(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}

func keyOf(tbl string, k item) string {
	sort := sv(k, "uuid") + sv(k, "id")
	return tbl + "|" + sv(k, "application") + "|" + sort
}

func clone(it item) item {
	out := item{}
	for k, v := range it {
		out[k] = v
	}
	return out
}

func (f *fakeDB) PutItem(_ context.Context, in *awsdb.PutItemInput, _ ...func(*awsdb.Options)) (*awsdb.PutItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.items[keyOf(*in.TableName, in.Item)] = clone(in.Item)
	return &awsdb.PutItemOutput{}, nil
}

func (f *fakeDB) GetItem(_ context.Context, in *awsdb.GetItemInput, _ ...func(*awsdb.Options)) (*awsdb.GetItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &awsdb.GetItemOutput{Item: clone(f.items[keyOf(*in.TableName, in.Key)])}, nil
}

func (f *fakeDB) DeleteItem(_ context.Context, in *awsdb.DeleteItemInput, _ ...func(*awsdb.Options)) (*awsdb.DeleteItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := keyOf(*in.TableName, in.Key)
	old := f.items[k]
	delete(f.items, k)
	return &awsdb.DeleteItemOutput{Attributes: old}, nil
}

func (f *fakeDB) Query(_ context.Context, in *awsdb.QueryInput, _ ...func(*awsdb.Options)) (*awsdb.QueryOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := *in.TableName + "|" + sv(in.ExpressionAttributeValues, ":app") + "|"
	var out []item
	for k, it := range f.items {
		if strings.HasPrefix(k, prefix) {
			out = append(out, clone(it))
		}
	}
	return &awsdb.QueryOutput{Items: out}, nil
}

func numOf(it item, k string) int64 {
	if v, ok := it[k].(*types.AttributeValueMemberN); ok {
		n, _ := strconv.ParseInt(v.Value, 10, 64)
		return n
	}
	return 0
}

func (f *fakeDB) UpdateItem(_ context.Context, in *awsdb.UpdateItemInput, _ ...func(*awsdb.Options)) (*awsdb.UpdateItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := keyOf(*in.TableName, in.Key)
	it, ok := f.items[k]
	if !ok {
		return nil, &types.ConditionalCheckFailedException{Message: aws.String("missing")}
	}
	v := in.ExpressionAttributeValues
	num := func(x int64) types.AttributeValue {
		return &types.AttributeValueMemberN{Value: strconv.FormatInt(x, 10)}
	}
	set := func(name string, add []string, remove []string) {
		cur := map[string]bool{}
		if s, ok := it[name].(*types.AttributeValueMemberSS); ok {
			for _, x := range s.Value {
				cur[x] = true
			}
		}
		for _, x := range add {
			cur[x] = true
		}
		for _, x := range remove {
			delete(cur, x)
		}
		if len(cur) == 0 {
			delete(it, name)
			return
		}
		var vals []string
		for x := range cur {
			vals = append(vals, x)
		}
		it[name] = &types.AttributeValueMemberSS{Value: vals}
	}
	job := func() []string { return v[":job"].(*types.AttributeValueMemberSS).Value }
	switch *in.UpdateExpression {
	case "ADD total_jobs :n, pending_jobs :n REMOVE finished_at":
		it["total_jobs"] = num(numOf(it, "total_jobs") + numOf(v, ":n"))
		it["pending_jobs"] = num(numOf(it, "pending_jobs") + numOf(v, ":n"))
		delete(it, "finished_at")
	case "ADD pending_jobs :minus DELETE failed_job_ids :job":
		it["pending_jobs"] = num(numOf(it, "pending_jobs") - 1)
		set("failed_job_ids", nil, job())
	case "ADD failed_jobs :one, failed_job_ids :job":
		it["failed_jobs"] = num(numOf(it, "failed_jobs") + 1)
		set("failed_job_ids", job(), nil)
	case "SET finished_at = :at":
		it["finished_at"] = v[":at"]
	case "SET cancelled_at = :at, finished_at = :at":
		it["cancelled_at"], it["finished_at"] = v[":at"], v[":at"]
	default:
		panic("unexpected update " + *in.UpdateExpression)
	}
	return &awsdb.UpdateItemOutput{Attributes: clone(it)}, nil
}

func TestFailedJobStore(t *testing.T) {
	s := dynamodb.NewFailedJobStore(newFake(), dynamodb.Options{TTL: time.Hour})
	at := time.Unix(1_800_000_000, 0)
	for i, q := range []string{"a", "b"} {
		if err := s.Log(ctx, perago.FailedJob{ID: "id" + strconv.Itoa(i), Connection: "sqs", Queue: q, Payload: []byte("{}"), Exception: "e", FailedAt: at.Add(time.Duration(i) * time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}
	all, _ := s.All(ctx)
	if len(all) != 2 || all[0].ID != "id1" {
		t.Fatalf("all = %+v", all)
	}
	if ids, _ := s.IDs(ctx, "a"); len(ids) != 1 || ids[0] != "id0" {
		t.Fatalf("ids = %v", ids)
	}
	if f, err := s.Find(ctx, "id0"); err != nil || f.Queue != "a" {
		t.Fatalf("find = %+v %v", f, err)
	}
	if _, err := s.Find(ctx, "nope"); !perago.IsCode(err, perago.FailedJobNotFound) {
		t.Fatal(err)
	}
	if n, _ := s.Prune(ctx, at.Add(30*time.Minute)); n != 1 {
		t.Fatalf("prune = %d", n)
	}
	if ok, _ := s.Forget(ctx, "id1"); !ok {
		t.Fatal("forget")
	}
}

func TestBatchStore(t *testing.T) {
	s := dynamodb.NewBatchStore(newFake(), dynamodb.Options{})
	at := time.Unix(1_800_000_000, 0)
	b := &perago.Batch{ID: "b1", Name: "n", CreatedAt: at, Options: perago.BatchOptions{AllowFailures: true}}
	if err := s.Store(ctx, b); err != nil {
		t.Fatal(err)
	}
	_ = s.IncrementTotalJobs(ctx, "b1", 2)
	if got, _ := s.RecordFailure(ctx, "b1", "j1"); got.FailedJobs != 1 || len(got.FailedJobIDs) != 1 {
		t.Fatalf("failure = %+v", got)
	}
	got, err := s.RecordSuccess(ctx, "b1", "j1")
	if err != nil || got.PendingJobs != 1 || len(got.FailedJobIDs) != 0 || !got.Options.AllowFailures {
		t.Fatalf("success = %+v %v", got, err)
	}
	if _, err := s.RecordSuccess(ctx, "missing", "j"); !perago.IsCode(err, perago.BatchNotFound) {
		t.Fatalf("missing batch = %v", err)
	}
	_ = s.Cancel(ctx, "b1", at.Add(time.Minute))
	f, _ := s.Find(ctx, "b1")
	if f.CancelledAt == nil || f.FinishedAt == nil {
		t.Fatalf("cancel = %+v", f)
	}
	_ = s.Store(ctx, &perago.Batch{ID: "b2", CreatedAt: at.Add(time.Hour)})
	if list, _ := s.List(ctx, 1, ""); len(list) != 1 || list[0].ID != "b2" {
		t.Fatalf("list = %v", list)
	}
	if n, _ := s.PruneCancelled(ctx, at.Add(2*time.Minute)); n != 1 {
		t.Fatalf("prune cancelled = %d", n)
	}
	if n, _ := s.PruneUnfinished(ctx, at.Add(2*time.Hour)); n != 1 {
		t.Fatalf("prune unfinished = %d", n)
	}
}

func TestManagerWithDynamoStores(t *testing.T) {
	db := newFake()
	q := perago.NewMemoryDriver(0)
	m, err := perago.New(perago.Config{Default: "mem", Connections: map[string]perago.Connection{"mem": {Driver: q}},
		Failed: dynamodb.NewFailedJobStore(db, dynamodb.Options{}), Batches: dynamodb.NewBatchStore(db, dynamodb.Options{})})
	if err != nil {
		t.Fatal(err)
	}
	var done sync.WaitGroup
	done.Add(1)
	_, err = m.Batch(perago.Closure(func(context.Context) error { return nil })).Then(func(context.Context, *perago.Batch) { done.Done() }).Dispatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Work(ctx, perago.WorkerOptions{StopWhenEmpty: true}); err != nil {
		t.Fatal(err)
	}
	done.Wait()
}

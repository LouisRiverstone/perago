// Package dynamodb stores perago's failed jobs and job batches in DynamoDB (Laravel's
// "Storing Failed Jobs / Batches in DynamoDB").
//
// Each table has a string partition key "application" and a string sort key: "uuid" for failed
// jobs, "id" for batches. Enable DynamoDB TTL on the "ttl" attribute to expire items on their own.
package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/LouisRiverstone/perago"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsdb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Client is the part of *dynamodb.Client the stores use.
type Client interface {
	PutItem(ctx context.Context, in *awsdb.PutItemInput, opts ...func(*awsdb.Options)) (*awsdb.PutItemOutput, error)
	GetItem(ctx context.Context, in *awsdb.GetItemInput, opts ...func(*awsdb.Options)) (*awsdb.GetItemOutput, error)
	DeleteItem(ctx context.Context, in *awsdb.DeleteItemInput, opts ...func(*awsdb.Options)) (*awsdb.DeleteItemOutput, error)
	UpdateItem(ctx context.Context, in *awsdb.UpdateItemInput, opts ...func(*awsdb.Options)) (*awsdb.UpdateItemOutput, error)
	Query(ctx context.Context, in *awsdb.QueryInput, opts ...func(*awsdb.Options)) (*awsdb.QueryOutput, error)
}

// Options configures the stores.
type Options struct {
	// Table name.
	Table string
	// Application is the partition key value, so several apps can share a table.
	Application string
	// TTL sets the "ttl" attribute this long after an item is written (0 = never).
	TTL time.Duration
	Now func() time.Time
}

func (o *Options) defaults(table string) {
	if o.Table == "" {
		o.Table = table
	}
	if o.Application == "" {
		o.Application = "perago"
	}
	if o.Now == nil {
		o.Now = time.Now
	}
}

func s(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func n(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}

func str(item map[string]types.AttributeValue, k string) string {
	if v, ok := item[k].(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}

func num(item map[string]types.AttributeValue, k string) (int64, bool) {
	v, ok := item[k].(*types.AttributeValueMemberN)
	if !ok {
		return 0, false
	}
	x, err := strconv.ParseInt(v.Value, 10, 64)
	return x, err == nil
}

func strSet(item map[string]types.AttributeValue, k string) []string {
	if v, ok := item[k].(*types.AttributeValueMemberSS); ok {
		return append([]string(nil), v.Value...)
	}
	return nil
}

func fail(err error, op string) error {
	if err == nil {
		return nil
	}
	return perago.Wrap(perago.StoreFailed, err, "dynamodb %s", op)
}

func queryAll(ctx context.Context, c Client, table, app string) ([]map[string]types.AttributeValue, error) {
	var out []map[string]types.AttributeValue
	var start map[string]types.AttributeValue
	for {
		res, err := c.Query(ctx, &awsdb.QueryInput{
			TableName:                 aws.String(table),
			KeyConditionExpression:    aws.String("application = :app"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":app": s(app)},
			ExclusiveStartKey:         start,
			ConsistentRead:            aws.Bool(true),
		})
		if err != nil {
			return nil, fail(err, "query")
		}
		out = append(out, res.Items...)
		if len(res.LastEvaluatedKey) == 0 {
			return out, nil
		}
		start = res.LastEvaluatedKey
	}
}

// FailedJobStore keeps failed jobs in DynamoDB.
type FailedJobStore struct {
	c Client
	o Options
}

// NewFailedJobStore returns the DynamoDB failed job store (table "failed_jobs" by default).
func NewFailedJobStore(c Client, o Options) *FailedJobStore {
	o.defaults("failed_jobs")
	return &FailedJobStore{c: c, o: o}
}

func (st *FailedJobStore) key(id string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"application": s(st.o.Application), "uuid": s(id)}
}

func (st *FailedJobStore) Log(ctx context.Context, f perago.FailedJob) error {
	item := st.key(f.ID)
	item["connection"], item["queue"] = s(f.Connection), s(f.Queue)
	item["payload"], item["exception"] = s(string(f.Payload)), s(f.Exception)
	item["failed_at"] = n(f.FailedAt.Unix())
	if st.o.TTL > 0 {
		item["ttl"] = n(st.o.Now().Add(st.o.TTL).Unix())
	}
	_, err := st.c.PutItem(ctx, &awsdb.PutItemInput{TableName: aws.String(st.o.Table), Item: item})
	return fail(err, "put failed job")
}

func toFailed(item map[string]types.AttributeValue) perago.FailedJob {
	at, _ := num(item, "failed_at")
	return perago.FailedJob{
		ID: str(item, "uuid"), Connection: str(item, "connection"), Queue: str(item, "queue"),
		Payload: []byte(str(item, "payload")), Exception: str(item, "exception"), FailedAt: time.Unix(at, 0),
	}
}

func (st *FailedJobStore) All(ctx context.Context) ([]perago.FailedJob, error) {
	items, err := queryAll(ctx, st.c, st.o.Table, st.o.Application)
	if err != nil {
		return nil, err
	}
	out := make([]perago.FailedJob, 0, len(items))
	for _, it := range items {
		out = append(out, toFailed(it))
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].FailedAt.After(out[j].FailedAt) })
	return out, nil
}

func (st *FailedJobStore) Find(ctx context.Context, id string) (*perago.FailedJob, error) {
	res, err := st.c.GetItem(ctx, &awsdb.GetItemInput{TableName: aws.String(st.o.Table), Key: st.key(id), ConsistentRead: aws.Bool(true)})
	if err != nil {
		return nil, fail(err, "get failed job")
	}
	if len(res.Item) == 0 {
		return nil, perago.Newf(perago.FailedJobNotFound, "failed job %s not found", id)
	}
	f := toFailed(res.Item)
	return &f, nil
}

func (st *FailedJobStore) IDs(ctx context.Context, queue string) ([]string, error) {
	all, err := st.All(ctx)
	if err != nil {
		return nil, err
	}
	var ids []string
	for i := len(all) - 1; i >= 0; i-- {
		if queue == "" || all[i].Queue == queue {
			ids = append(ids, all[i].ID)
		}
	}
	return ids, nil
}

func (st *FailedJobStore) Forget(ctx context.Context, id string) (bool, error) {
	res, err := st.c.DeleteItem(ctx, &awsdb.DeleteItemInput{TableName: aws.String(st.o.Table), Key: st.key(id), ReturnValues: types.ReturnValueAllOld})
	if err != nil {
		return false, fail(err, "delete failed job")
	}
	return len(res.Attributes) > 0, nil
}

func (st *FailedJobStore) remove(ctx context.Context, match func(perago.FailedJob) bool) (int64, error) {
	all, err := st.All(ctx)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, f := range all {
		if !match(f) {
			continue
		}
		if _, err := st.Forget(ctx, f.ID); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func (st *FailedJobStore) Flush(ctx context.Context, olderThan time.Duration) (int64, error) {
	if olderThan <= 0 {
		return st.remove(ctx, func(perago.FailedJob) bool { return true })
	}
	return st.Prune(ctx, st.o.Now().Add(-olderThan))
}

func (st *FailedJobStore) Prune(ctx context.Context, before time.Time) (int64, error) {
	return st.remove(ctx, func(f perago.FailedJob) bool { return f.FailedAt.Before(before) })
}

// BatchStore keeps job batches in DynamoDB. Counter updates are single atomic UpdateItem calls.
type BatchStore struct {
	c Client
	o Options
}

// NewBatchStore returns the DynamoDB batch store (table "job_batches" by default).
func NewBatchStore(c Client, o Options) *BatchStore {
	o.defaults("job_batches")
	return &BatchStore{c: c, o: o}
}

func (st *BatchStore) key(id string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"application": s(st.o.Application), "id": s(id)}
}

func (st *BatchStore) Store(ctx context.Context, b *perago.Batch) error {
	opts, err := json.Marshal(b.Options)
	if err != nil {
		return perago.Wrap(perago.StoreFailed, err, "encode batch options")
	}
	item := st.key(b.ID)
	item["name"], item["options"] = s(b.Name), s(string(opts))
	item["total_jobs"], item["pending_jobs"], item["failed_jobs"] = n(int64(b.TotalJobs)), n(int64(b.PendingJobs)), n(int64(b.FailedJobs))
	item["created_at"] = n(b.CreatedAt.Unix())
	if len(b.FailedJobIDs) > 0 {
		item["failed_job_ids"] = &types.AttributeValueMemberSS{Value: b.FailedJobIDs}
	}
	if st.o.TTL > 0 {
		item["ttl"] = n(st.o.Now().Add(st.o.TTL).Unix())
	}
	_, err = st.c.PutItem(ctx, &awsdb.PutItemInput{TableName: aws.String(st.o.Table), Item: item})
	return fail(err, "put batch")
}

func toBatch(item map[string]types.AttributeValue) (*perago.Batch, error) {
	b := &perago.Batch{ID: str(item, "id"), Name: str(item, "name"), FailedJobIDs: strSet(item, "failed_job_ids")}
	total, _ := num(item, "total_jobs")
	pending, _ := num(item, "pending_jobs")
	failed, _ := num(item, "failed_jobs")
	b.TotalJobs, b.PendingJobs, b.FailedJobs = int(total), int(pending), int(failed)
	created, _ := num(item, "created_at")
	b.CreatedAt = time.Unix(created, 0)
	if v, ok := num(item, "cancelled_at"); ok {
		t := time.Unix(v, 0)
		b.CancelledAt = &t
	}
	if v, ok := num(item, "finished_at"); ok {
		t := time.Unix(v, 0)
		b.FinishedAt = &t
	}
	if o := str(item, "options"); o != "" {
		if err := json.Unmarshal([]byte(o), &b.Options); err != nil {
			return nil, perago.Wrap(perago.StoreFailed, err, "decode batch options")
		}
	}
	return b, nil
}

func (st *BatchStore) Find(ctx context.Context, id string) (*perago.Batch, error) {
	res, err := st.c.GetItem(ctx, &awsdb.GetItemInput{TableName: aws.String(st.o.Table), Key: st.key(id), ConsistentRead: aws.Bool(true)})
	if err != nil {
		return nil, fail(err, "get batch")
	}
	if len(res.Item) == 0 {
		return nil, perago.Newf(perago.BatchNotFound, "batch %s not found", id)
	}
	return toBatch(res.Item)
}

func (st *BatchStore) all(ctx context.Context) ([]*perago.Batch, error) {
	items, err := queryAll(ctx, st.c, st.o.Table, st.o.Application)
	if err != nil {
		return nil, err
	}
	out := make([]*perago.Batch, 0, len(items))
	for _, it := range items {
		b, err := toBatch(it)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (st *BatchStore) List(ctx context.Context, limit int, before string) ([]*perago.Batch, error) {
	all, err := st.all(ctx)
	if err != nil {
		return nil, err
	}
	if before != "" {
		for i, b := range all {
			if b.ID == before {
				all = all[i+1:]
				break
			}
		}
	}
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

func (st *BatchStore) update(ctx context.Context, id, expr string, values map[string]types.AttributeValue) (*perago.Batch, error) {
	res, err := st.c.UpdateItem(ctx, &awsdb.UpdateItemInput{
		TableName: aws.String(st.o.Table), Key: st.key(id),
		UpdateExpression: aws.String(expr), ExpressionAttributeValues: values,
		ConditionExpression: aws.String("attribute_exists(id)"),
		ReturnValues:        types.ReturnValueAllNew,
	})
	if err != nil {
		var cond *types.ConditionalCheckFailedException
		if errors.As(err, &cond) {
			return nil, perago.Newf(perago.BatchNotFound, "batch %s not found", id)
		}
		return nil, fail(err, "update batch")
	}
	return toBatch(res.Attributes)
}

func (st *BatchStore) IncrementTotalJobs(ctx context.Context, id string, count int) error {
	_, err := st.update(ctx, id, "ADD total_jobs :n, pending_jobs :n REMOVE finished_at", map[string]types.AttributeValue{":n": n(int64(count))})
	return err
}

func (st *BatchStore) RecordSuccess(ctx context.Context, id, jobID string) (*perago.Batch, error) {
	return st.update(ctx, id, "ADD pending_jobs :minus DELETE failed_job_ids :job",
		map[string]types.AttributeValue{":minus": n(-1), ":job": &types.AttributeValueMemberSS{Value: []string{jobID}}})
}

func (st *BatchStore) RecordFailure(ctx context.Context, id, jobID string) (*perago.Batch, error) {
	return st.update(ctx, id, "ADD failed_jobs :one, failed_job_ids :job",
		map[string]types.AttributeValue{":one": n(1), ":job": &types.AttributeValueMemberSS{Value: []string{jobID}}})
}

func (st *BatchStore) MarkFinished(ctx context.Context, id string, at time.Time) error {
	_, err := st.update(ctx, id, "SET finished_at = :at", map[string]types.AttributeValue{":at": n(at.Unix())})
	return err
}

func (st *BatchStore) Cancel(ctx context.Context, id string, at time.Time) error {
	_, err := st.update(ctx, id, "SET cancelled_at = :at, finished_at = :at", map[string]types.AttributeValue{":at": n(at.Unix())})
	return err
}

func (st *BatchStore) Delete(ctx context.Context, id string) error {
	_, err := st.c.DeleteItem(ctx, &awsdb.DeleteItemInput{TableName: aws.String(st.o.Table), Key: st.key(id)})
	return fail(err, "delete batch")
}

func (st *BatchStore) prune(ctx context.Context, match func(*perago.Batch) bool) (int64, error) {
	all, err := st.all(ctx)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, b := range all {
		if !match(b) {
			continue
		}
		if err := st.Delete(ctx, b.ID); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func (st *BatchStore) Prune(ctx context.Context, before time.Time) (int64, error) {
	return st.prune(ctx, func(b *perago.Batch) bool { return b.FinishedAt != nil && b.FinishedAt.Before(before) })
}

func (st *BatchStore) PruneUnfinished(ctx context.Context, before time.Time) (int64, error) {
	return st.prune(ctx, func(b *perago.Batch) bool { return b.FinishedAt == nil && b.CreatedAt.Before(before) })
}

func (st *BatchStore) PruneCancelled(ctx context.Context, before time.Time) (int64, error) {
	return st.prune(ctx, func(b *perago.Batch) bool { return b.CancelledAt != nil && b.CancelledAt.Before(before) })
}

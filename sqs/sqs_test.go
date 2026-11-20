package sqs_test

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LouisRiverstone/perago"
	"github.com/LouisRiverstone/perago/internal/drivertest"
	"github.com/LouisRiverstone/perago/sqs"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type fakeMsg struct {
	id, body, receipt, group, dedup string
	attrs                           map[string]types.MessageAttributeValue
	visibleAt                       time.Time
	receives                        int
	inFlight                        bool
}

// fakeSQS is an in-memory SQS with visibility timeouts and receive counts.
type fakeSQS struct {
	mu         sync.Mutex
	clock      *clock
	visibility time.Duration
	queues     map[string][]*fakeMsg
	n          int
	sent       []*awssqs.SendMessageInput
}

func newFake(c *clock) *fakeSQS {
	return &fakeSQS{clock: c, visibility: 5 * time.Second, queues: map[string][]*fakeMsg{}}
}

func (f *fakeSQS) SendMessage(_ context.Context, in *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, in)
	f.n++
	id := "m" + strconv.Itoa(f.n)
	url := aws.ToString(in.QueueUrl)
	f.queues[url] = append(f.queues[url], &fakeMsg{
		id: id, body: aws.ToString(in.MessageBody), attrs: in.MessageAttributes,
		group: aws.ToString(in.MessageGroupId), dedup: aws.ToString(in.MessageDeduplicationId),
		visibleAt: f.clock.Now().Add(time.Duration(in.DelaySeconds) * time.Second),
	})
	return &awssqs.SendMessageOutput{MessageId: aws.String(id)}, nil
}

func (f *fakeSQS) SendMessageBatch(ctx context.Context, in *awssqs.SendMessageBatchInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageBatchOutput, error) {
	out := &awssqs.SendMessageBatchOutput{}
	for _, e := range in.Entries {
		r, _ := f.SendMessage(ctx, &awssqs.SendMessageInput{QueueUrl: in.QueueUrl, MessageBody: e.MessageBody, DelaySeconds: e.DelaySeconds, MessageAttributes: e.MessageAttributes, MessageGroupId: e.MessageGroupId, MessageDeduplicationId: e.MessageDeduplicationId})
		out.Successful = append(out.Successful, types.SendMessageBatchResultEntry{Id: e.Id, MessageId: r.MessageId})
	}
	return out, nil
}

func (f *fakeSQS) ReceiveMessage(_ context.Context, in *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.clock.Now()
	for _, m := range f.queues[aws.ToString(in.QueueUrl)] {
		if m.visibleAt.After(now) {
			continue
		}
		m.receives++
		m.inFlight = true
		m.visibleAt = now.Add(f.visibility)
		f.n++
		m.receipt = "r" + strconv.Itoa(f.n)
		return &awssqs.ReceiveMessageOutput{Messages: []types.Message{{
			MessageId: aws.String(m.id), Body: aws.String(m.body), ReceiptHandle: aws.String(m.receipt),
			MessageAttributes: m.attrs,
			Attributes:        map[string]string{"ApproximateReceiveCount": strconv.Itoa(m.receives)},
		}}}, nil
	}
	return &awssqs.ReceiveMessageOutput{}, nil
}

func (f *fakeSQS) find(url, receipt string) (int, *fakeMsg) {
	for i, m := range f.queues[url] {
		if m.receipt == receipt {
			return i, m
		}
	}
	return -1, nil
}

func (f *fakeSQS) DeleteMessage(_ context.Context, in *awssqs.DeleteMessageInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	url := aws.ToString(in.QueueUrl)
	if i, _ := f.find(url, aws.ToString(in.ReceiptHandle)); i >= 0 {
		f.queues[url] = append(f.queues[url][:i], f.queues[url][i+1:]...)
	}
	return &awssqs.DeleteMessageOutput{}, nil
}

func (f *fakeSQS) ChangeMessageVisibility(_ context.Context, in *awssqs.ChangeMessageVisibilityInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, m := f.find(aws.ToString(in.QueueUrl), aws.ToString(in.ReceiptHandle)); m != nil {
		m.visibleAt = f.clock.Now().Add(time.Duration(in.VisibilityTimeout) * time.Second)
		m.inFlight = in.VisibilityTimeout > 0
	}
	return &awssqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeSQS) GetQueueAttributes(_ context.Context, in *awssqs.GetQueueAttributesInput, _ ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.clock.Now()
	var visible, delayed, inflight int
	for _, m := range f.queues[aws.ToString(in.QueueUrl)] {
		switch {
		case !m.visibleAt.After(now):
			visible++
		case m.inFlight:
			inflight++
		default:
			delayed++
		}
	}
	return &awssqs.GetQueueAttributesOutput{Attributes: map[string]string{
		"ApproximateNumberOfMessages":           strconv.Itoa(visible),
		"ApproximateNumberOfMessagesDelayed":    strconv.Itoa(delayed),
		"ApproximateNumberOfMessagesNotVisible": strconv.Itoa(inflight),
	}}, nil
}

func (f *fakeSQS) PurgeQueue(_ context.Context, in *awssqs.PurgeQueueInput, _ ...func(*awssqs.Options)) (*awssqs.PurgeQueueOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.queues, aws.ToString(in.QueueUrl))
	return &awssqs.PurgeQueueOutput{}, nil
}

const prefix = "https://sqs.us-east-1.amazonaws.com/123456789012/"

func TestDriverConformance(t *testing.T) {
	drivertest.Run(t, func(t *testing.T) (perago.Driver, func(time.Duration)) {
		c := &clock{t: time.Unix(1_800_000_000, 0)}
		return sqs.NewDriver(newFake(c), sqs.Options{Prefix: prefix, Now: c.Now}), c.Advance
	}, 5*time.Second)
}

func TestURLs(t *testing.T) {
	d := sqs.NewDriver(newFake(&clock{}), sqs.Options{Prefix: prefix, Suffix: "-prod"})
	if got := d.URL("emails"); got != prefix+"emails-prod" {
		t.Fatal(got)
	}
	if got := d.URL("orders.fifo"); got != prefix+"orders-prod.fifo" {
		t.Fatal(got)
	}
	if got := d.URL("https://example.com/q"); got != "https://example.com/q" {
		t.Fatal(got)
	}
}

func TestFIFOGroupsAndDeduplication(t *testing.T) {
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	f := newFake(c)
	d := sqs.NewDriver(f, sqs.Options{Prefix: prefix, Now: c.Now})
	ctx := context.Background()
	if _, err := d.Push(ctx, perago.Message{ID: perago.NewUUID(), Queue: "orders.fifo", Payload: []byte("x"), Group: "customer-1", DeduplicationID: "d1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Push(ctx, perago.Message{ID: "u2", Queue: "orders.fifo", Payload: []byte("y")}); err != nil {
		t.Fatal(err)
	}
	if g, dd := aws.ToString(f.sent[0].MessageGroupId), aws.ToString(f.sent[0].MessageDeduplicationId); g != "customer-1" || dd != "d1" {
		t.Fatalf("group=%s dedup=%s", g, dd)
	}
	if g, dd := aws.ToString(f.sent[1].MessageGroupId), aws.ToString(f.sent[1].MessageDeduplicationId); g != "orders.fifo" || dd != "u2" {
		t.Fatalf("defaults: group=%s dedup=%s", g, dd)
	}
	if _, err := d.Push(ctx, perago.Message{ID: "u3", Queue: "orders.fifo", Delay: time.Second}); err == nil {
		t.Fatal("FIFO delay accepted")
	}
	// Fair queues: a group on a standard queue is passed through.
	_, _ = d.Push(ctx, perago.Message{ID: "u4", Queue: "standard", Group: "tenant-9"})
	if g := aws.ToString(f.sent[len(f.sent)-1].MessageGroupId); g != "tenant-9" || f.sent[len(f.sent)-1].MessageDeduplicationId != nil {
		t.Fatalf("fair queue group=%s", g)
	}
}

func TestLongDelayIsRequeued(t *testing.T) {
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	d := sqs.NewDriver(newFake(c), sqs.Options{Prefix: prefix, Now: c.Now})
	ctx := context.Background()
	if _, err := d.Push(ctx, perago.Message{ID: perago.NewUUID(), Queue: "q", Payload: []byte("later"), Delay: 40 * time.Minute}); err != nil {
		t.Fatal(err)
	}
	for _, step := range []time.Duration{16 * time.Minute, 16 * time.Minute} {
		c.Advance(step)
		if r, err := d.Pop(ctx, "q"); r != nil || err != nil {
			t.Fatalf("popped before due: %v %v", r, err)
		}
	}
	c.Advance(10 * time.Minute)
	r, err := d.Pop(ctx, "q")
	if err != nil || r == nil || string(r.Payload()) != "later" {
		t.Fatalf("due job: %v %v", r, err)
	}
}

func TestOverflow(t *testing.T) {
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	f := newFake(c)
	store := sqs.CacheOverflow{Cache: perago.NewMemoryCache()}
	d := sqs.NewDriver(f, sqs.Options{Prefix: prefix, Now: c.Now, Overflow: sqs.Overflow{Enabled: true, Store: store, Threshold: 10, DeleteAfterProcessing: true}})
	ctx := context.Background()
	big := strings.Repeat("z", 50)
	id := perago.NewUUID()
	if _, err := d.Push(ctx, perago.Message{ID: id, Queue: "q", Payload: []byte(big)}); err != nil {
		t.Fatal(err)
	}
	if body := aws.ToString(f.sent[0].MessageBody); body == big {
		t.Fatal("large payload went to SQS")
	}
	r, err := d.Pop(ctx, "q")
	if err != nil || string(r.Payload()) != big {
		t.Fatalf("overflow pop: %v", err)
	}
	if err := r.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "perago:sqs-overflow:"+id); err == nil {
		t.Fatal("overflowed payload not deleted")
	}
}

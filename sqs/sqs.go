// Package sqs is perago's Amazon SQS queue driver: standard, FIFO and fair queues, message groups,
// deduplication, long polling and overflow storage for payloads larger than SQS accepts.
//
// SQS's visibility timeout plays the part of retry_after: configure it on the queue to be longer
// than the job timeout.
package sqs

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/LouisRiverstone/perago"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// Client is the part of *sqs.Client the driver uses.
type Client interface {
	SendMessage(ctx context.Context, in *awssqs.SendMessageInput, opts ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
	SendMessageBatch(ctx context.Context, in *awssqs.SendMessageBatchInput, opts ...func(*awssqs.Options)) (*awssqs.SendMessageBatchOutput, error)
	ReceiveMessage(ctx context.Context, in *awssqs.ReceiveMessageInput, opts ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *awssqs.DeleteMessageInput, opts ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *awssqs.ChangeMessageVisibilityInput, opts ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
	GetQueueAttributes(ctx context.Context, in *awssqs.GetQueueAttributesInput, opts ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error)
	PurgeQueue(ctx context.Context, in *awssqs.PurgeQueueInput, opts ...func(*awssqs.Options)) (*awssqs.PurgeQueueOutput, error)
}

// OverflowStore keeps payloads too large for SQS (Laravel's SQS overflow storage).
type OverflowStore interface {
	Put(ctx context.Context, key string, payload []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	// Flush deletes every stored payload (flush_on_clear).
	Flush(ctx context.Context) error
}

// Overflow configures overflow storage.
type Overflow struct {
	Enabled bool
	Store   OverflowStore
	// Always stores every payload, not just large ones.
	Always bool
	// Threshold is the payload size that overflows (default 1 MiB, SQS's limit).
	Threshold int
	// DeleteAfterProcessing removes the stored payload once the job is deleted.
	DeleteAfterProcessing bool
	// FlushOnClear flushes the store on queue:clear.
	FlushOnClear bool
}

// Options configures NewDriver.
type Options struct {
	// Prefix is prepended to queue names that are not URLs
	// ("https://sqs.us-east-1.amazonaws.com/123456789012/").
	Prefix string
	// Suffix is appended to queue names that are not URLs (per-environment queues).
	Suffix string
	// WaitTime enables long polling (up to 20s).
	WaitTime time.Duration
	Overflow Overflow
	Now      func() time.Time
}

// MaxDelay is the longest delay SQS takes; longer delays are re-queued until due.
const MaxDelay = 15 * time.Minute

const (
	availableAtAttr = "perago_available_at"
	overflowAttr    = "perago_overflow"
)

// Driver is the SQS queue driver.
type Driver struct {
	c   Client
	o   Options
	now func() time.Time
}

// NewDriver returns the SQS queue driver.
func NewDriver(c Client, o Options) *Driver {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Overflow.Threshold <= 0 {
		o.Overflow.Threshold = 1024 * 1024
	}
	return &Driver{c: c, o: o, now: o.Now}
}

// URL resolves a queue name to its queue URL.
func (d *Driver) URL(queue string) string {
	if strings.HasPrefix(queue, "https://") || strings.HasPrefix(queue, "http://") {
		return queue
	}
	suffix := d.o.Suffix
	if strings.HasSuffix(queue, ".fifo") && suffix != "" {
		queue = strings.TrimSuffix(queue, ".fifo")
		suffix += ".fifo"
	}
	return strings.TrimSuffix(d.o.Prefix, "/") + "/" + queue + suffix
}

func isFIFO(url string) bool { return strings.HasSuffix(url, ".fifo") }

func fail(err error, op string) error {
	if err == nil {
		return nil
	}
	return perago.Wrap(perago.DriverFailed, err, "sqs %s", op)
}

func overflowKey(id string) string { return "perago:sqs-overflow:" + id }

// message prepares the SendMessage fields of m.
func (d *Driver) message(ctx context.Context, m perago.Message) (body string, delay int32, attrs map[string]types.MessageAttributeValue, group, dedup *string, err error) {
	url := d.URL(m.Queue)
	attrs = map[string]types.MessageAttributeValue{}
	body = string(m.Payload)
	if d.o.Overflow.Enabled && (d.o.Overflow.Always || len(m.Payload) >= d.o.Overflow.Threshold) {
		if d.o.Overflow.Store == nil {
			return "", 0, nil, nil, nil, perago.Newf(perago.InvalidConfig, "sqs overflow is enabled without a store")
		}
		key := overflowKey(m.ID)
		if err := d.o.Overflow.Store.Put(ctx, key, m.Payload); err != nil {
			return "", 0, nil, nil, nil, err
		}
		body = "{}"
		attrs[overflowAttr] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(key)}
	}
	if m.Delay > 0 && isFIFO(url) {
		return "", 0, nil, nil, nil, perago.Newf(perago.PushFailed, "SQS FIFO queues do not take per-message delays (%s)", m.Queue)
	}
	if m.Delay > MaxDelay {
		at := d.now().Add(m.Delay).Unix()
		attrs[availableAtAttr] = types.MessageAttributeValue{DataType: aws.String("Number"), StringValue: aws.String(strconv.FormatInt(at, 10))}
		m.Delay = MaxDelay
	}
	delay = int32(m.Delay / time.Second)
	if m.Group != "" || isFIFO(url) {
		g := m.Group
		if g == "" {
			g = m.Queue
		}
		group = aws.String(g)
	}
	if isFIFO(url) {
		id := m.DeduplicationID
		if id == "" {
			id = m.ID
		}
		dedup = aws.String(id)
	}
	if len(attrs) == 0 {
		attrs = nil
	}
	return body, delay, attrs, group, dedup, nil
}

func (d *Driver) Push(ctx context.Context, m perago.Message) (string, error) {
	if m.ID == "" {
		m.ID = perago.NewUUID()
	}
	body, delay, attrs, group, dedup, err := d.message(ctx, m)
	if err != nil {
		return "", err
	}
	out, err := d.c.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl: aws.String(d.URL(m.Queue)), MessageBody: aws.String(body), DelaySeconds: delay,
		MessageAttributes: attrs, MessageGroupId: group, MessageDeduplicationId: dedup,
	})
	if err != nil {
		return "", fail(err, "send")
	}
	return aws.ToString(out.MessageId), nil
}

// PushBulk sends messages in batches of ten per queue.
func (d *Driver) PushBulk(ctx context.Context, ms []perago.Message) ([]string, error) {
	byQueue := map[string][]perago.Message{}
	var order []string
	for _, m := range ms {
		if m.ID == "" {
			m.ID = perago.NewUUID()
		}
		if _, ok := byQueue[m.Queue]; !ok {
			order = append(order, m.Queue)
		}
		byQueue[m.Queue] = append(byQueue[m.Queue], m)
	}
	var ids []string
	for _, q := range order {
		group := byQueue[q]
		for start := 0; start < len(group); start += 10 {
			end := min(start+10, len(group))
			entries := make([]types.SendMessageBatchRequestEntry, 0, end-start)
			for i, m := range group[start:end] {
				body, delay, attrs, g, dedup, err := d.message(ctx, m)
				if err != nil {
					return ids, err
				}
				entries = append(entries, types.SendMessageBatchRequestEntry{
					Id: aws.String(strconv.Itoa(i)), MessageBody: aws.String(body), DelaySeconds: delay,
					MessageAttributes: attrs, MessageGroupId: g, MessageDeduplicationId: dedup,
				})
			}
			out, err := d.c.SendMessageBatch(ctx, &awssqs.SendMessageBatchInput{QueueUrl: aws.String(d.URL(q)), Entries: entries})
			if err != nil {
				return ids, fail(err, "send batch")
			}
			if len(out.Failed) > 0 {
				return ids, perago.Newf(perago.PushFailed, "sqs rejected %d messages: %s", len(out.Failed), aws.ToString(out.Failed[0].Message))
			}
			for _, s := range out.Successful {
				ids = append(ids, aws.ToString(s.MessageId))
			}
		}
	}
	return ids, nil
}

func (d *Driver) Pop(ctx context.Context, queue string) (perago.Reserved, error) {
	url := d.URL(queue)
	out, err := d.c.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl: aws.String(url), MaxNumberOfMessages: 1,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
		MessageAttributeNames:       []string{"All"},
		WaitTimeSeconds:             int32(d.o.WaitTime / time.Second),
	})
	if err != nil {
		return nil, fail(err, "receive")
	}
	if len(out.Messages) == 0 {
		return nil, nil
	}
	msg := out.Messages[0]
	r := &reserved{d: d, queue: queue, url: url, msg: msg, payload: []byte(aws.ToString(msg.Body))}
	r.attempts, _ = strconv.Atoi(msg.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	if r.attempts < 1 {
		r.attempts = 1
	}
	if v, ok := msg.MessageAttributes[availableAtAttr]; ok {
		at, _ := strconv.ParseInt(aws.ToString(v.StringValue), 10, 64)
		if wait := time.Unix(at, 0).Sub(d.now()); wait > 0 {
			return nil, d.redelay(ctx, r, wait)
		}
	}
	if v, ok := msg.MessageAttributes[overflowAttr]; ok {
		r.overflow = aws.ToString(v.StringValue)
		if d.o.Overflow.Store == nil {
			return nil, perago.Newf(perago.InvalidConfig, "sqs message uses overflow storage, but no store is configured")
		}
		if r.payload, err = d.o.Overflow.Store.Get(ctx, r.overflow); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// redelay sends a message whose delay exceeded MaxDelay back with the time it still has to wait.
func (d *Driver) redelay(ctx context.Context, r *reserved, wait time.Duration) error {
	attrs := r.msg.MessageAttributes
	if wait <= MaxDelay {
		delete(attrs, availableAtAttr)
	}
	_, err := d.c.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl: aws.String(r.url), MessageBody: r.msg.Body, DelaySeconds: int32(min(wait, MaxDelay) / time.Second), MessageAttributes: attrs,
	})
	if err != nil {
		return fail(err, "re-delay")
	}
	return r.deleteMessage(ctx)
}

func (d *Driver) attrs(ctx context.Context, queue string, names ...types.QueueAttributeName) (map[string]string, error) {
	out, err := d.c.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(d.URL(queue)), AttributeNames: names})
	if err != nil {
		return nil, fail(err, "attributes")
	}
	return out.Attributes, nil
}

func (d *Driver) attr(ctx context.Context, queue string, name types.QueueAttributeName) (int64, error) {
	a, err := d.attrs(ctx, queue, name)
	if err != nil {
		return 0, err
	}
	n, _ := strconv.ParseInt(a[string(name)], 10, 64)
	return n, nil
}

func (d *Driver) Size(ctx context.Context, queue string) (int64, error) {
	names := []types.QueueAttributeName{
		types.QueueAttributeNameApproximateNumberOfMessages,
		types.QueueAttributeNameApproximateNumberOfMessagesDelayed,
		types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
	}
	a, err := d.attrs(ctx, queue, names...)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, n := range names {
		v, _ := strconv.ParseInt(a[string(n)], 10, 64)
		total += v
	}
	return total, nil
}

func (d *Driver) PendingSize(ctx context.Context, queue string) (int64, error) {
	return d.attr(ctx, queue, types.QueueAttributeNameApproximateNumberOfMessages)
}

func (d *Driver) DelayedSize(ctx context.Context, queue string) (int64, error) {
	return d.attr(ctx, queue, types.QueueAttributeNameApproximateNumberOfMessagesDelayed)
}

func (d *Driver) ReservedSize(ctx context.Context, queue string) (int64, error) {
	return d.attr(ctx, queue, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)
}

// Clear purges the queue (SQS allows one purge per minute).
func (d *Driver) Clear(ctx context.Context, queue string) (int64, error) {
	n, err := d.Size(ctx, queue)
	if err != nil {
		return 0, err
	}
	if _, err := d.c.PurgeQueue(ctx, &awssqs.PurgeQueueInput{QueueUrl: aws.String(d.URL(queue))}); err != nil {
		return 0, fail(err, "purge")
	}
	if d.o.Overflow.FlushOnClear && d.o.Overflow.Store != nil {
		if err := d.o.Overflow.Store.Flush(ctx); err != nil {
			return n, err
		}
	}
	return n, nil
}

type reserved struct {
	d        *Driver
	queue    string
	url      string
	msg      types.Message
	payload  []byte
	attempts int
	overflow string
}

func (r *reserved) ID() string      { return aws.ToString(r.msg.MessageId) }
func (r *reserved) Queue() string   { return r.queue }
func (r *reserved) Payload() []byte { return r.payload }
func (r *reserved) Attempts() int   { return r.attempts }

func (r *reserved) deleteMessage(ctx context.Context) error {
	_, err := r.d.c.DeleteMessage(ctx, &awssqs.DeleteMessageInput{QueueUrl: aws.String(r.url), ReceiptHandle: r.msg.ReceiptHandle})
	return fail(err, "delete")
}

func (r *reserved) Delete(ctx context.Context) error {
	if err := r.deleteMessage(ctx); err != nil {
		return err
	}
	if r.overflow != "" && r.d.o.Overflow.DeleteAfterProcessing && r.d.o.Overflow.Store != nil {
		return r.d.o.Overflow.Store.Delete(ctx, r.overflow)
	}
	return nil
}

// Release makes the message visible again after delay (at most 12 hours).
func (r *reserved) Release(ctx context.Context, delay time.Duration) error {
	secs := int32(min(delay, 12*time.Hour) / time.Second)
	_, err := r.d.c.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(r.url), ReceiptHandle: r.msg.ReceiptHandle, VisibilityTimeout: secs})
	return fail(err, "release")
}

// CacheOverflow stores overflowed payloads in a perago.Cache (a Redis cache, typically).
// Flush is a no-op: delete payloads with DeleteAfterProcessing.
type CacheOverflow struct {
	Cache perago.Cache
	TTL   time.Duration
}

func (c CacheOverflow) Put(ctx context.Context, key string, payload []byte) error {
	return c.Cache.Put(ctx, key, string(payload), c.TTL)
}

func (c CacheOverflow) Get(ctx context.Context, key string) ([]byte, error) {
	v, ok, err := c.Cache.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, perago.Newf(perago.PayloadInvalid, "overflowed payload %s is gone", key)
	}
	return []byte(v), nil
}

func (c CacheOverflow) Delete(ctx context.Context, key string) error { return c.Cache.Forget(ctx, key) }
func (c CacheOverflow) Flush(context.Context) error                  { return nil }

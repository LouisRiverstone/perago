// Package redis is perago's Redis queue driver and Redis cache (locks, rate limiters, worker
// signals). It works with a single server, Sentinel or Cluster through redis.UniversalClient.
//
// On Redis Cluster, wrap queue names in a hash tag ("{default}") so every key of a queue lands
// on the same slot. Give each application its own Prefix (or database) so two apps never take
// each other's jobs.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/LouisRiverstone/perago"
	goredis "github.com/redis/go-redis/v9"
)

// DriverOptions configures NewDriver.
type DriverOptions struct {
	// Prefix starts every key ("perago:" by default).
	Prefix string
	// RetryAfter is how long a reserved job may run before another worker takes it again
	// (retry_after, default 90s).
	RetryAfter time.Duration
	// BlockFor makes Pop wait this long for a job with BLPOP when the queue is empty
	// (block_for). 0 polls.
	BlockFor time.Duration
	// Now replaces the clock (tests).
	Now func() time.Time
}

// Driver is the Redis queue driver. A queue is a list of ready jobs plus sorted sets of delayed
// and reserved jobs, moved between them atomically by Lua scripts, like Laravel's RedisQueue.
type Driver struct {
	c          goredis.UniversalClient
	prefix     string
	retryAfter time.Duration
	blockFor   time.Duration
	now        func() time.Time
}

// NewDriver returns the Redis queue driver.
func NewDriver(c goredis.UniversalClient, o DriverOptions) *Driver {
	if o.Prefix == "" {
		o.Prefix = "perago:"
	}
	if o.RetryAfter <= 0 {
		o.RetryAfter = 90 * time.Second
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Driver{c: c, prefix: o.Prefix, retryAfter: o.RetryAfter, blockFor: o.BlockFor, now: o.Now}
}

type keys struct{ list, delayed, reserved, attempts string }

func (d *Driver) keys(queue string) keys {
	base := d.prefix + "queues:" + queue
	return keys{list: base, delayed: base + ":delayed", reserved: base + ":reserved", attempts: base + ":attempts"}
}

const idLen = 36

// element is how a job is stored: its 36-character id followed by the payload, so scripts can
// find the id without decoding JSON.
func element(id string, payload []byte) (string, error) {
	if len(id) != idLen {
		return "", perago.Newf(perago.PushFailed, "redis job ids must be UUIDs, got %q", id)
	}
	return id + string(payload), nil
}

func ms(t time.Time) int64 { return t.UnixMilli() }

func wrap(err error, op string) error {
	if err == nil {
		return nil
	}
	return perago.Wrap(perago.DriverFailed, err, "redis %s", op)
}

func (d *Driver) Push(ctx context.Context, m perago.Message) (string, error) {
	if m.ID == "" {
		m.ID = perago.NewUUID()
	}
	el, err := element(m.ID, m.Payload)
	if err != nil {
		return "", err
	}
	k := d.keys(m.Queue)
	if m.Delay > 0 {
		return m.ID, wrap(d.c.ZAdd(ctx, k.delayed, goredis.Z{Score: float64(ms(d.now().Add(m.Delay))), Member: el}).Err(), "push")
	}
	return m.ID, wrap(d.c.RPush(ctx, k.list, el).Err(), "push")
}

// PushBulk pushes every message in one pipeline.
func (d *Driver) PushBulk(ctx context.Context, msgs []perago.Message) ([]string, error) {
	ids := make([]string, len(msgs))
	_, err := d.c.Pipelined(ctx, func(p goredis.Pipeliner) error {
		for i, m := range msgs {
			if m.ID == "" {
				m.ID = perago.NewUUID()
			}
			el, err := element(m.ID, m.Payload)
			if err != nil {
				return err
			}
			ids[i] = m.ID
			k := d.keys(m.Queue)
			if m.Delay > 0 {
				p.ZAdd(ctx, k.delayed, goredis.Z{Score: float64(ms(d.now().Add(m.Delay))), Member: el})
				continue
			}
			p.RPush(ctx, k.list, el)
		}
		return nil
	})
	return ids, wrap(err, "bulk push")
}

var popScript = goredis.NewScript(`
local function migrate(from, to, now)
  local due = redis.call('zrangebyscore', from, '-inf', now)
  if #due > 0 then
    redis.call('zremrangebyscore', from, '-inf', now)
    for i = 1, #due, 100 do
      redis.call('rpush', to, unpack(due, i, math.min(i + 99, #due)))
    end
  end
end
migrate(KEYS[2], KEYS[1], ARGV[1])
migrate(KEYS[3], KEYS[1], ARGV[1])
local job = redis.call('lpop', KEYS[1])
if not job then
  return false
end
redis.call('zadd', KEYS[3], ARGV[2], job)
local attempts = redis.call('hincrby', KEYS[4], string.sub(job, 1, 36), 1)
return {job, attempts}
`)

var reserveScript = goredis.NewScript(`
redis.call('zadd', KEYS[1], ARGV[2], ARGV[1])
return redis.call('hincrby', KEYS[2], string.sub(ARGV[1], 1, 36), 1)
`)

var releaseScript = goredis.NewScript(`
redis.call('zrem', KEYS[2], ARGV[1])
redis.call('zadd', KEYS[1], ARGV[2], ARGV[1])
return 1
`)

var deleteScript = goredis.NewScript(`
redis.call('zrem', KEYS[1], ARGV[1])
redis.call('hdel', KEYS[2], string.sub(ARGV[1], 1, 36))
return 1
`)

func (d *Driver) Pop(ctx context.Context, queue string) (perago.Reserved, error) {
	k := d.keys(queue)
	now := d.now()
	until := ms(now.Add(d.retryAfter))
	res, err := popScript.Run(ctx, d.c, []string{k.list, k.delayed, k.reserved, k.attempts}, ms(now), until).Slice()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return nil, wrap(err, "pop")
	}
	if len(res) == 2 {
		el, _ := res[0].(string)
		n, _ := res[1].(int64)
		return d.reserved(queue, el, int(n)), nil
	}
	if d.blockFor <= 0 {
		return nil, nil
	}
	got, err := d.c.BLPop(ctx, d.blockFor, k.list).Result()
	if errors.Is(err, goredis.Nil) || len(got) < 2 {
		return nil, nil
	}
	if err != nil {
		return nil, wrap(err, "blpop")
	}
	n, err := reserveScript.Run(ctx, d.c, []string{k.reserved, k.attempts}, got[1], until).Int64()
	if err != nil {
		return nil, wrap(err, "reserve")
	}
	return d.reserved(queue, got[1], int(n)), nil
}

func (d *Driver) reserved(queue, el string, attempts int) *reserved {
	return &reserved{d: d, queue: queue, el: el, attempts: attempts}
}

func (d *Driver) Size(ctx context.Context, queue string) (int64, error) {
	k := d.keys(queue)
	var l, dl, rs *goredis.IntCmd
	_, err := d.c.Pipelined(ctx, func(p goredis.Pipeliner) error {
		l, dl, rs = p.LLen(ctx, k.list), p.ZCard(ctx, k.delayed), p.ZCard(ctx, k.reserved)
		return nil
	})
	if err != nil {
		return 0, wrap(err, "size")
	}
	return l.Val() + dl.Val() + rs.Val(), nil
}

func (d *Driver) Clear(ctx context.Context, queue string) (int64, error) {
	n, err := d.Size(ctx, queue)
	if err != nil {
		return 0, err
	}
	k := d.keys(queue)
	return n, wrap(d.c.Del(ctx, k.list, k.delayed, k.reserved, k.attempts).Err(), "clear")
}

func (d *Driver) PendingSize(ctx context.Context, queue string) (int64, error) {
	n, err := d.c.LLen(ctx, d.keys(queue).list).Result()
	return n, wrap(err, "pending size")
}

func (d *Driver) DelayedSize(ctx context.Context, queue string) (int64, error) {
	n, err := d.c.ZCard(ctx, d.keys(queue).delayed).Result()
	return n, wrap(err, "delayed size")
}

func (d *Driver) ReservedSize(ctx context.Context, queue string) (int64, error) {
	n, err := d.c.ZCard(ctx, d.keys(queue).reserved).Result()
	return n, wrap(err, "reserved size")
}

func (d *Driver) OldestPending(ctx context.Context, queue string) (time.Time, error) {
	el, err := d.c.LIndex(ctx, d.keys(queue).list, 0).Result()
	if errors.Is(err, goredis.Nil) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, wrap(err, "oldest pending")
	}
	var p struct {
		CreatedAt int64 `json:"createdAt"`
	}
	if len(el) > idLen && json.Unmarshal([]byte(el[idLen:]), &p) == nil && p.CreatedAt > 0 {
		return time.Unix(p.CreatedAt, 0), nil
	}
	return d.now(), nil
}

type reserved struct {
	d        *Driver
	queue    string
	el       string
	attempts int
}

func (r *reserved) ID() string      { return r.el[:idLen] }
func (r *reserved) Queue() string   { return r.queue }
func (r *reserved) Payload() []byte { return []byte(r.el[idLen:]) }
func (r *reserved) Attempts() int   { return r.attempts }

func (r *reserved) Delete(ctx context.Context) error {
	k := r.d.keys(r.queue)
	return wrap(deleteScript.Run(ctx, r.d.c, []string{k.reserved, k.attempts}, r.el).Err(), "delete")
}

func (r *reserved) Release(ctx context.Context, delay time.Duration) error {
	k := r.d.keys(r.queue)
	at := ms(r.d.now().Add(delay))
	return wrap(releaseScript.Run(ctx, r.d.c, []string{k.delayed, k.reserved}, r.el, at).Err(), "release")
}

// Cache is a perago.Cache on Redis.
type Cache struct {
	c      goredis.UniversalClient
	prefix string
}

// NewCache returns a Redis cache whose keys start with prefix ("perago:cache:" when empty).
func NewCache(c goredis.UniversalClient, prefix string) *Cache {
	if prefix == "" {
		prefix = "perago:cache:"
	}
	return &Cache{c: c, prefix: prefix}
}

func cacheErr(err error, op, key string) error {
	if err == nil {
		return nil
	}
	return perago.Wrap(perago.CacheFailed, err, "redis %s %q", op, key)
}

func (c *Cache) Get(ctx context.Context, key string) (string, bool, error) {
	v, err := c.c.Get(ctx, c.prefix+key).Result()
	if errors.Is(err, goredis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, cacheErr(err, "get", key)
	}
	return v, true, nil
}

func (c *Cache) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	return cacheErr(c.c.Set(ctx, c.prefix+key, value, max(ttl, 0)).Err(), "put", key)
}

func (c *Cache) Add(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	ok, err := c.c.SetNX(ctx, c.prefix+key, value, max(ttl, 0)).Result()
	return ok, cacheErr(err, "add", key)
}

var incrScript = goredis.NewScript(`
if redis.call('exists', KEYS[1]) == 0 then
  if tonumber(ARGV[2]) > 0 then
    redis.call('set', KEYS[1], ARGV[1], 'PX', ARGV[2])
  else
    redis.call('set', KEYS[1], ARGV[1])
  end
  return tonumber(ARGV[1])
end
return redis.call('incrby', KEYS[1], ARGV[1])
`)

func (c *Cache) Increment(ctx context.Context, key string, by int64, ttl time.Duration) (int64, error) {
	n, err := incrScript.Run(ctx, c.c, []string{c.prefix + key}, by, max(ttl, 0).Milliseconds()).Int64()
	return n, cacheErr(err, "increment", key)
}

func (c *Cache) Forget(ctx context.Context, key string) error {
	return cacheErr(c.c.Del(ctx, c.prefix+key).Err(), "forget", key)
}

var forgetIfScript = goredis.NewScript(`
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
end
return 0
`)

func (c *Cache) ForgetIf(ctx context.Context, key, value string) (bool, error) {
	n, err := forgetIfScript.Run(ctx, c.c, []string{c.prefix + key}, value).Int64()
	return n == 1, cacheErr(err, "forget", key)
}

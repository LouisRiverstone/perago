package perago

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"time"
)

// Payload is the envelope a driver stores. Data is the job's JSON; when the job is
// ShouldBeEncrypted, Data is empty and Secret holds it sealed with AES-256-GCM.
type Payload struct {
	UUID        string          `json:"uuid"`
	DisplayName string          `json:"displayName"`
	Job         string          `json:"job"`
	Data        json.RawMessage `json:"data,omitempty"`
	Secret      string          `json:"secret,omitempty"`

	// Settings resolved at dispatch. MaxTries nil means "use the worker's --tries".
	MaxTries      *int    `json:"maxTries,omitempty"`
	MaxExceptions int     `json:"maxExceptions,omitempty"`
	Backoff       []int64 `json:"backoff,omitempty"` // milliseconds
	Timeout       int64   `json:"timeout,omitempty"` // milliseconds; 0 uses the worker's --timeout
	RetryUntil    int64   `json:"retryUntil,omitempty"`
	FailOnTimeout bool    `json:"failOnTimeout,omitempty"`
	CountCrashes  bool    `json:"countCrashes,omitempty"`
	DeleteMissing bool    `json:"deleteWhenMissingModels,omitempty"`

	Connection string `json:"connection"`
	Queue      string `json:"queue"`
	Group      string `json:"group,omitempty"`
	DedupID    string `json:"deduplicationId,omitempty"`

	// Chain holds the payloads of the jobs that run after this one; ChainCatch the callbacks
	// that run when any of them fails.
	Chain           []json.RawMessage `json:"chain,omitempty"`
	ChainConnection string            `json:"chainConnection,omitempty"`
	ChainQueue      string            `json:"chainQueue,omitempty"`
	ChainCatch      []json.RawMessage `json:"chainCatch,omitempty"`

	BatchID string `json:"batchId,omitempty"`
	// Delay is the delay of a chained job, applied when the chain reaches it (milliseconds).
	Delay int64 `json:"delay,omitempty"`

	UniqueKey             string `json:"uniqueKey,omitempty"`
	UniqueUntilProcessing bool   `json:"uniqueUntilProcessing,omitempty"`
	DebounceKey           string `json:"debounceKey,omitempty"`

	CreatedAt int64 `json:"createdAt"`
}

func (p *Payload) backoffFor(attempt int, fallback []time.Duration) time.Duration {
	if len(p.Backoff) > 0 {
		i := min(max(attempt-1, 0), len(p.Backoff)-1)
		return time.Duration(p.Backoff[i]) * time.Millisecond
	}
	if len(fallback) == 0 {
		return 0
	}
	i := min(max(attempt-1, 0), len(fallback)-1)
	return fallback[i]
}

// DecodePayload parses a stored payload.
func DecodePayload(raw []byte) (*Payload, error) {
	var p Payload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, Wrap(PayloadInvalid, err, "decode payload")
	}
	if p.Job == "" {
		return nil, Newf(PayloadInvalid, "payload has no job name")
	}
	return &p, nil
}

func (p *Payload) encode() ([]byte, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return nil, Wrap(PayloadInvalid, err, "encode payload for %s", p.Job)
	}
	return b, nil
}

func seal(key []byte, plain []byte) (string, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", Wrap(PayloadInvalid, err, "nonce")
	}
	return base64.StdEncoding.EncodeToString(gcm.Seal(nonce, nonce, plain, nil)), nil
}

func open(key []byte, sealed string) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	raw, err := base64.StdEncoding.DecodeString(sealed)
	if err != nil {
		return nil, Wrap(PayloadDecryptFailed, err, "decode ciphertext")
	}
	if len(raw) < gcm.NonceSize() {
		return nil, Newf(PayloadDecryptFailed, "ciphertext too short")
	}
	plain, err := gcm.Open(nil, raw[:gcm.NonceSize()], raw[gcm.NonceSize():], nil)
	if err != nil {
		return nil, Wrap(PayloadDecryptFailed, err, "decrypt payload")
	}
	return plain, nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, Newf(InvalidConfig, "encrypted jobs need Config.Key with 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, Wrap(InvalidConfig, err, "cipher")
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, Wrap(InvalidConfig, err, "gcm")
	}
	return gcm, nil
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	const hex = "0123456789abcdef"
	out := make([]byte, 36)
	j := 0
	for i, c := range b {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out[j] = '-'
			j++
		}
		out[j], out[j+1] = hex[c>>4], hex[c&0x0f]
		j += 2
	}
	return string(out)
}

// NewUUID returns a random version 4 UUID (drivers use it for job ids).
func NewUUID() string { return newUUID() }

package dht

import (
	"errors"
	"hash/maphash"
	"sync"
	"time"

	"github.com/purehyperbole/dht/protocol"
)

var (
	// ErrRequestTimeout returned when a pending request has not recevied a response before the TTL period
	ErrRequestTimeout = errors.New("request timeout")
)

// a pending request
type request struct {
	callback func(event *protocol.Event, err error)
	ttl      time.Time
}

// cache tracks asynchronous event requests
type cache struct {
	requests sync.Map
	hasher   sync.Pool
}

func newCache(refresh time.Duration) *cache {
	seed := maphash.MakeSeed()

	c := &cache{
		hasher: sync.Pool{
			New: func() any {
				var hasher maphash.Hash
				hasher.SetSeed(seed)
				return &hasher
			},
		},
	}

	go c.cleanup(refresh)

	return c
}

func (c *cache) set(key []byte, ttl time.Time, cb func(*protocol.Event, error)) {
	r := &request{callback: cb, ttl: ttl}

	h := c.hasher.Get().(*maphash.Hash)

	h.Reset()
	h.Write(key)

	k := h.Sum64()

	c.hasher.Put(h)

	c.requests.Store(k, r)
}

func (c *cache) pop(key []byte) (func(*protocol.Event, error), bool) {
	h := c.hasher.Get().(*maphash.Hash)

	h.Reset()
	h.Write(key)

	k := h.Sum64()

	c.hasher.Put(h)

	r, ok := c.requests.Load(k)
	if ok {
		c.requests.Delete(k)
		return r.(*request).callback, ok
	}

	return nil, false
}

func (c *cache) cleanup(refresh time.Duration) {
	// TODO : this is going to block everything, not good
	for {
		time.Sleep(refresh)

		now := time.Now()

		c.requests.Range(func(key, value any) bool {
			v := value.(*request)

			if now.After(v.ttl) {
				v.callback(nil, ErrRequestTimeout)
				c.requests.Delete(key)
			}

			return true
		})
	}
}

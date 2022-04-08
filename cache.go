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
	// TODO: take another look at sync.Map, check if memory usage/GC has improved
	requests map[uint64]*request
	hasher   maphash.Hash
	seed     maphash.Seed
	mu       sync.Mutex
}

func newCache(refresh time.Duration) *cache {
	var hasher maphash.Hash

	seed := maphash.MakeSeed()

	hasher.SetSeed(seed)

	c := &cache{
		requests: make(map[uint64]*request),
		hasher:   hasher,
		seed:     seed,
	}

	go c.cleanup(refresh)

	return c
}

func (c *cache) set(key []byte, ttl time.Time, cb func(*protocol.Event, error)) {
	r := &request{callback: cb, ttl: ttl}

	c.mu.Lock()

	c.hasher.Reset()
	c.hasher.Write(key)

	c.requests[c.hasher.Sum64()] = r

	c.mu.Unlock()
}

func (c *cache) pop(key []byte) (func(*protocol.Event, error), bool) {
	c.mu.Lock()

	c.hasher.Reset()
	c.hasher.Write(key)

	k := c.hasher.Sum64()

	r, ok := c.requests[k]
	if ok {
		delete(c.requests, k)
		c.mu.Unlock()
		return r.callback, ok
	}

	c.mu.Unlock()

	return nil, false
}

func (c *cache) cleanup(refresh time.Duration) {
	// TODO : this is going to block everything, not good
	for {
		time.Sleep(refresh)

		now := time.Now()

		c.mu.Lock()

		for k, v := range c.requests {
			if v.ttl.After(now) {
				v.callback(nil, ErrRequestTimeout)
				delete(c.requests, k)
			}
		}

		c.mu.Unlock()
	}
}

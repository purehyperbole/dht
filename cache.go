package dht

import (
	"errors"
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
	requests map[string]*request
	mu       sync.Mutex
}

func newCache(refresh time.Duration) *cache {
	c := &cache{
		requests: make(map[string]*request),
	}

	go c.cleanup(refresh)

	return c
}

func (c *cache) get(key []byte) (func(*protocol.Event, error), bool) {
	c.mu.Lock()
	// TODO : try to avoid this unecessary allocation
	r, ok := c.requests[string(key)]
	c.mu.Unlock()

	return r.callback, ok
}

func (c *cache) set(key []byte, ttl time.Time, cb func(*protocol.Event, error)) {
	c.mu.Lock()
	// TODO : try to avoid this unecessary allocation
	c.requests[string(key)] = &request{callback: cb, ttl: ttl}
	c.mu.Unlock()
}

func (c *cache) pop(key []byte) (func(*protocol.Event, error), bool) {
	c.mu.Lock()
	// TODO : try to avoid this unecessary allocation
	k := string(key)

	r, ok := c.requests[k]
	if ok {
		delete(c.requests, k)
	}

	c.mu.Unlock()

	return r.callback, ok
}

func (c *cache) remove(key []byte) {
	c.mu.Lock()
	// TODO : try to avoid this unecessary allocation
	delete(c.requests, string(key))
	c.mu.Unlock()
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

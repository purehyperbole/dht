package dht

import (
	"sync"
	"time"
)

type value struct {
	data    []byte
	ttl     time.Duration
	expires time.Time
}

// implement simple storage for now storage
type storage struct {
	store map[string]*value
	mu    sync.Mutex
}

func newStorage() *storage {
	s := &storage{
		store: make(map[string]*value),
	}

	go s.cleanup()

	return s
}

func (s *storage) get(k []byte) ([]byte, bool) {
	s.mu.Lock()
	v, ok := s.store[string(k)]
	s.mu.Unlock()

	if !ok {
		return nil, false
	}

	return v.data, ok
}

func (s *storage) set(k, v []byte, ttl time.Duration) {
	s.mu.Lock()

	// we keep a copy of the value as it's actually
	// read from a buffer that's going to be reused
	// so we need to store this as a copy to avoid
	// it getting overwritten by other data
	// we don't need to do this for the key
	// as the string() call allocates a new underlying
	// array and creates a copy for us

	vc := make([]byte, len(v))
	copy(vc, v)

	s.store[string(k)] = &value{
		data:    vc,
		ttl:     ttl,
		expires: time.Now().Add(ttl),
	}

	s.mu.Unlock()
}

func (s *storage) iterate(cb func(k, v []byte, ttl time.Duration) bool) {
	s.mu.Lock()

	for k, v := range s.store {
		if !cb([]byte(k), v.data, v.ttl) {
			s.mu.Unlock()
			return
		}
	}

	s.mu.Unlock()
}

func (s *storage) cleanup() {
	for {
		// scan the storage to check for values that have expired
		time.Sleep(time.Minute)

		now := time.Now()

		s.mu.Lock()

		for k, v := range s.store {
			if v.expires.After(now) {
				delete(s.store, k)
			}
		}

		s.mu.Unlock()
	}
}

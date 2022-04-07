package dht

import (
	"sync"
	"time"
)

type value struct {
	data []byte
	ttl  time.Time
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

func (s *storage) set(k, v []byte, ttl time.Time) {
	s.mu.Lock()

	s.store[string(k)] = &value{
		data: v,
		ttl:  ttl,
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
			if v.ttl.After(now) {
				delete(s.store, k)
			}
		}

		s.mu.Unlock()
	}
}

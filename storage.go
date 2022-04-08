package dht

import (
	"hash/maphash"
	"sync"
	"time"
)

// Storage defines the storage interface used by the DLT
type Storage interface {
	Get(key []byte) ([]byte, bool)
	Set(key, value []byte, ttl time.Duration) bool
	Iterate(cb func(key, value []byte, ttl time.Duration) bool)
}

type value struct {
	key     []byte
	data    []byte
	ttl     time.Duration
	expires time.Time
}

// implement simple storage for now storage
type storage struct {
	store  map[uint64]*value
	hasher maphash.Hash
	seed   maphash.Seed
	mu     sync.Mutex
}

func newInMemoryStorage() *storage {
	var hasher maphash.Hash

	seed := maphash.MakeSeed()

	hasher.SetSeed(seed)

	s := &storage{
		store:  make(map[uint64]*value),
		hasher: hasher,
		seed:   seed,
	}

	go s.cleanup()

	return s
}

// Get gets a key by its id
func (s *storage) Get(k []byte) ([]byte, bool) {
	s.mu.Lock()
	s.hasher.Reset()
	s.hasher.Write(k)

	v, ok := s.store[s.hasher.Sum64()]
	s.mu.Unlock()

	if !ok {
		return nil, false
	}

	return v.data, ok
}

// Set sets a key value pair for a given ttl
func (s *storage) Set(k, v []byte, ttl time.Duration) bool {
	// we keep a copy of the key and value as it's actually
	// read from a buffer that's going to be reused
	// so we need to store this as a copy to avoid
	// it getting overwritten by other data

	kc := make([]byte, len(k))
	copy(kc, k)

	vc := make([]byte, len(v))
	copy(vc, v)

	s.mu.Lock()

	s.hasher.Reset()
	s.hasher.Write(k)

	s.store[s.hasher.Sum64()] = &value{
		key:     kc,
		data:    vc,
		ttl:     ttl,
		expires: time.Now().Add(ttl),
	}

	s.mu.Unlock()

	return true
}

// Iterate iterates over keys in the storage
func (s *storage) Iterate(cb func(k, v []byte, ttl time.Duration) bool) {
	s.mu.Lock()

	for _, v := range s.store {
		if !cb(v.key, v.data, v.ttl) {
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

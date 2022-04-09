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
	// store  map[uint64]*value
	store  sync.Map
	hasher maphash.Hash
	seed   maphash.Seed
	mu     sync.Mutex
}

func newInMemoryStorage() *storage {
	// TODO : this will probably cause collisions
	// that need to be handled!
	var hasher maphash.Hash

	seed := maphash.MakeSeed()

	hasher.SetSeed(seed)

	s := &storage{
		store:  sync.Map{},
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
	key := s.hasher.Sum64()

	s.mu.Unlock()

	v, ok := s.store.Load(key)
	if !ok {
		return nil, false
	}

	return v.(*value).data, true
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
	s.hasher.Write(kc)
	key := s.hasher.Sum64()

	s.mu.Unlock()

	s.store.Store(key, &value{
		key:     kc,
		data:    vc,
		ttl:     ttl,
		expires: time.Now().Add(ttl),
	})

	return true
}

// Iterate iterates over keys in the storage
func (s *storage) Iterate(cb func(k, v []byte, ttl time.Duration) bool) {
	s.store.Range(func(ky any, vl any) bool {
		val := vl.(*value)
		return cb(val.key, val.data, val.ttl)
	})
}

func (s *storage) cleanup() {
	for {
		// scan the storage to check for values that have expired
		time.Sleep(time.Minute)

		now := time.Now()

		s.store.Range(func(ky any, vl any) bool {
			val := vl.(*value)
			if val.expires.After(now) {
				s.store.Delete(ky)
			}
			return true
		})
	}
}

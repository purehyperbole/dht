package dht

import (
	"hash/maphash"
	"sync"
	"time"
)

// Storage defines the storage interface used by the DLT
type Storage interface {
	Get(key []byte) ([]*Value, bool)
	Set(key, value []byte, ttl time.Duration) bool
	Iterate(cb func(value *Value) bool)
}

// Value represents the value to be stored
type Value struct {
	Key     []byte
	Value   []byte
	TTL     time.Duration
	Created time.Time
	expires time.Time
}

type item struct {
	contains map[uint64]struct{}
	values   []*Value
	mu       sync.Mutex
}

// implement simple storage for now storage
type storage struct {
	store  sync.Map
	hasher sync.Pool
}

func newInMemoryStorage() *storage {
	// TODO : this will probably cause collisions
	// that need to be handled!
	seed := maphash.MakeSeed()

	s := &storage{
		store: sync.Map{},
		hasher: sync.Pool{
			New: func() any {
				var hasher maphash.Hash
				hasher.SetSeed(seed)
				return &hasher
			},
		},
	}

	go s.cleanup()

	return s
}

// Get gets a key by its id
func (s *storage) Get(k []byte) ([]*Value, bool) {
	h := s.hasher.Get().(*maphash.Hash)

	h.Reset()
	h.Write(k)
	key := h.Sum64()

	s.hasher.Put(h)

	v, ok := s.store.Load(key)
	if !ok {
		return nil, false
	}

	// TODO : actually store and return multiple values
	return v.(*item).values, true
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

	h := s.hasher.Get().(*maphash.Hash)
	defer s.hasher.Put(h)

	h.Reset()
	h.Write(k)
	key := h.Sum64()

	value := &Value{
		Key:     kc,
		Value:   vc,
		TTL:     ttl,
		expires: time.Now().Add(ttl),
	}

	actual, ok := s.store.LoadOrStore(key, &item{
		contains: map[uint64]struct{}{key: {}},
		values:   []*Value{value},
	})

	if !ok {
		return true
	}

	// hash the value so we can check if we have stored it already
	h.Reset()
	h.Write(v)
	vh := h.Sum64()

	existing := actual.(*item)

	existing.mu.Lock()
	defer existing.mu.Unlock()

	_, ok = existing.contains[vh]
	if ok {
		return true
	}

	// TODO this will be really slow, but good enough for now
	existing.values = append(existing.values, value)

	return true
}

// Iterate iterates over keys in the storage
func (s *storage) Iterate(cb func(v *Value) bool) {
	s.store.Range(func(ky any, vl any) bool {
		item := vl.(*item)

		item.mu.Lock()

		for i := range item.values {
			if !cb(item.values[i]) {
				return false
			}
		}

		item.mu.Unlock()

		return true
	})
}

func (s *storage) cleanup() {
	for {
		// scan the storage to check for values that have expired
		time.Sleep(time.Minute)

		now := time.Now()

		s.store.Range(func(ky any, vl any) bool {
			item := vl.(*item)
			item.mu.Lock()

			for i := range item.values {
				if item.values[i].expires.After(now) {
					s.store.Delete(ky)
				}
			}

			item.mu.Unlock()

			return true
		})
	}
}

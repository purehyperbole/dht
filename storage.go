package dht

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
)

// Storage defines the storage interface used by the DLT
type Storage interface {
	Get(key []byte, from time.Time) ([]*Value, bool)
	Set(key, value []byte, created time.Time, ttl time.Duration) bool
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
	//contains map[uint64]struct{}
	values []*Value
	// mu     sync.Mutex
}

func (i *item) insert(hash uint64, value *Value) bool {
	/*
		i.mu.Lock()
		defer i.mu.Unlock()

			_, ok := i.contains[hash]
			if ok {
				return true
			}

			// TODO this will be really slow, but good enough for now
			i.contains[hash] = struct{}{}
	*/

	i.values = append(i.values, value)

	return true
}

// implement simple storage for now storage
type storage struct {
	store  sync.Map
	hasher sync.Pool
	stored int64
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
func (s *storage) Get(k []byte, from time.Time) ([]*Value, bool) {
	h := s.hasher.Get().(*maphash.Hash)

	h.Reset()
	h.Write(k)
	key := h.Sum64()

	s.hasher.Put(h)

	v, ok := s.store.Load(key)
	if !ok {
		return nil, false
	}

	it := v.(*item)

	// if we don't need to filter the query, then return all values
	if from.IsZero() {
		return v.(*item).values, true
	}

	var index int

	// filter the query to values after a given date
	for i := 0; i < len(it.values); i++ {
		if it.values[i].Created.Before(from) {
			// TODO : might be a bit wonky if created from timestamps are not in order
			index++
		}
	}

	// we have no results left that are valid for the query
	if index >= len(it.values) {
		return nil, false
	}

	// TODO : actually store and return multiple values
	return it.values[index:], true
}

// Set sets a key value pair for a given ttl
func (s *storage) Set(k, v []byte, created time.Time, ttl time.Duration) bool {
	// we keep a copy of the key and value as it's actually
	// read from a buffer that's going to be reused
	// so we need to store this as a copy to avoid
	// it getting overwritten by other data

	kc := make([]byte, len(k))
	copy(kc, k)

	vc := make([]byte, len(v))
	copy(vc, v)

	h := s.hasher.Get().(*maphash.Hash)

	h.Reset()
	h.Write(k)
	key := h.Sum64()

	// hash the value so we can check if we have stored it already
	h.Reset()
	h.Write(v)
	vh := h.Sum64()

	s.hasher.Put(h)

	value := &Value{
		Key:     kc,
		Value:   vc,
		TTL:     ttl,
		Created: created,
		expires: time.Now().Add(ttl),
	}

	// loading first is apparently faster?
	actual, ok := s.store.Load(key)
	if ok {
		return actual.(*item).insert(vh, value)
	}

	actual, ok = s.store.LoadOrStore(key, &item{
		// contains: map[uint64]struct{}{vh: {}},
		values: []*Value{value},
	})

	if !ok {
		atomic.AddInt64(&s.stored, int64(len(vc)))
		return true
	}

	return actual.(*item).insert(vh, value)
}

// Iterate iterates over keys in the storage
func (s *storage) Iterate(cb func(v *Value) bool) {
	s.store.Range(func(ky any, vl any) bool {
		item := vl.(*item)

		//item.mu.Lock()

		for i := range item.values {
			if !cb(item.values[i]) {
				return false
			}
		}

		//item.mu.Unlock()

		return true
	})
}

func (s *storage) storedBytes() int64 {
	return atomic.LoadInt64(&s.stored)
}

func (s *storage) cleanup() {
	for {
		// scan the storage to check for values that have expired
		time.Sleep(time.Minute)

		now := time.Now()

		s.store.Range(func(ky any, vl any) bool {
			item := vl.(*item)
			//item.mu.Lock()

			for i := range item.values {
				if item.values[i].expires.After(now) {
					atomic.AddInt64(&s.stored, -int64(len(item.values[i].Value)))
					s.store.Delete(ky)
				}
			}

			//item.mu.Unlock()

			return true
		})
	}
}

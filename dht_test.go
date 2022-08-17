package dht

import (
	"crypto/rand"
	"errors"
	"fmt"
	"hash/maphash"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testHasher maphash.Hash
)

func wait(ch chan interface{}, timeout time.Duration) error {
	select {
	case <-ch:
		return nil
	case <-time.After(time.Second * 2):
		return errors.New("timeout")
	}
}

func init() {
	testHasher.SetSeed(maphash.MakeSeed())
}

func testHash(k []byte) uint64 {
	testHasher.Reset()
	testHasher.Write(k)
	return testHasher.Sum64()
}

type testStorage struct {
	store       sync.Map
	hasher      maphash.Hash
	mu          sync.Mutex
	setCallback func(value *Value)
}

func newTestStorage(scb func(value *Value)) *testStorage {
	var hasher maphash.Hash
	hasher.SetSeed(maphash.MakeSeed())

	return &testStorage{
		store:       sync.Map{},
		hasher:      hasher,
		setCallback: scb,
	}
}

// Get gets a key by its id
func (s *testStorage) Get(k []byte, from time.Time) ([]*Value, bool) {
	s.mu.Lock()

	s.hasher.Reset()
	s.hasher.Write(k)
	key := s.hasher.Sum64()

	s.mu.Unlock()

	v, ok := s.store.Load(key)
	if !ok {
		return nil, false
	}

	return []*Value{v.(*Value)}, ok
}

// Set sets a key value pair for a given ttl
func (s *testStorage) Set(k, v []byte, created time.Time, ttl time.Duration) bool {
	kc := make([]byte, len(k))
	copy(kc, k)

	vc := make([]byte, len(v))
	copy(vc, v)

	s.mu.Lock()

	s.hasher.Reset()
	s.hasher.Write(k)
	key := s.hasher.Sum64()

	s.mu.Unlock()

	val := &Value{
		Key:     kc,
		Value:   vc,
		TTL:     ttl,
		Created: created,
		expires: time.Now().Add(ttl),
	}

	s.store.Store(key, val)

	if s.setCallback != nil {
		s.setCallback(val)
	}

	return true
}

// Iterate iterates over keys in the storage
func (s *testStorage) Iterate(cb func(value *Value) bool) {
	s.store.Range(func(ky any, vl any) bool {
		return cb(vl.(*Value))
	})
}

func TestDHTBoostrap(t *testing.T) {
	bc := &Config{
		LocalID:       randomID(),
		ListenAddress: "127.0.0.1:9000",
	}

	// create a new bootstrap node
	bdht, err := New(bc)
	require.Nil(t, err)
	defer bdht.Close()

	// wait some time for the listeners to start
	time.Sleep(time.Millisecond * 200)

	for i := 0; i < 100; i++ {
		c := &Config{
			LocalID:       randomID(),
			ListenAddress: fmt.Sprintf("127.0.0.1:%d", 9001+i),
			BootstrapAddresses: []string{
				bc.ListenAddress,
			},
		}

		dht, err := New(c)
		require.Nil(t, err)
		defer dht.Close()
	}
}

func TestDHTLocalStoreFind(t *testing.T) {
	c := &Config{
		LocalID:       randomID(),
		ListenAddress: "127.0.0.1:9000",
	}

	// create a new dht with no nodes
	dht, err := New(c)
	require.Nil(t, err)
	defer dht.Close()

	// wait some time for the listeners to start
	time.Sleep(time.Millisecond * 200)

	// create a channel to handle our callback in a blocking way
	ch := make(chan error, 1)

	// attempt to store some data
	key := randomID()
	value := randomID()

	dht.Store(key, value, time.Hour, func(err error) {
		ch <- err
	})

	require.Nil(t, <-ch)

	var rv []byte

	dht.Find(key, func(v []byte, err error) {
		rv = make([]byte, len(v))
		copy(rv, v)
		ch <- err
	})

	require.Nil(t, <-ch)
	assert.Equal(t, value, rv)
}

func TestDHTLocalStoreFindMultiple(t *testing.T) {
	bc := &Config{
		LocalID:       randomID(),
		ListenAddress: "127.0.0.1:9000",
	}

	// create a new dht with no nodes
	bdht, err := New(bc)
	require.Nil(t, err)
	defer bdht.Close()

	// wait some time for the listeners to start
	time.Sleep(time.Millisecond * 200)

	for i := 0; i < 20; i++ {
		c := &Config{
			LocalID:       randomID(),
			ListenAddress: fmt.Sprintf("127.0.0.1:%d", 9001+i),
			BootstrapAddresses: []string{
				bc.ListenAddress,
			},
		}

		// create a new dht with no nodes
		dht, err := New(c)
		require.Nil(t, err)
		defer dht.Close()
	}

	// create a channel to handle our callback in a blocking way
	ch := make(chan error, 1000)

	var hasher maphash.Hash
	hasher.SetSeed(maphash.MakeSeed())

	// attempt to store some values to the same key
	key := randomID()

	values := make(map[uint64]struct{})

	for i := 0; i < 1000; i++ {
		value := make([]byte, 256)
		rand.Read(value)

		hasher.Reset()
		hasher.Write(value)
		v := hasher.Sum64()

		values[v] = struct{}{}

		bdht.Store(key, value, time.Hour, func(err error) {
			ch <- err
		})
	}

	for i := 0; i < 1000; i++ {
		require.Nil(t, <-ch)
	}

	type resp struct {
		data []byte
		err  error
	}

	// create a channel for our query responses
	ch2 := make(chan resp, 1000)

	bdht.Find(key, func(v []byte, err error) {
		rv := make([]byte, len(v))
		copy(rv, v)
		ch2 <- resp{data: rv, err: err}
	})

	for i := 0; i < 1000; i++ {
		r := <-ch2
		require.Nil(t, r.err)

		hasher.Reset()
		hasher.Write(r.data)
		v := hasher.Sum64()

		_, ok := values[v]
		assert.True(t, ok)

		delete(values, v)
	}
}

func TestDHTClusterStoreFind(t *testing.T) {
	bc := &Config{
		LocalID:       randomID(),
		ListenAddress: "127.0.0.1:9000",
		Listeners:     1,
	}

	// create a new bootstrap node
	bdht, err := New(bc)
	require.Nil(t, err)
	defer bdht.Close()

	// wait some time for the listeners to start
	time.Sleep(time.Millisecond * 200)

	// add some nodes to the network
	for i := 0; i < 2; i++ {
		c := &Config{
			LocalID:       randomID(),
			ListenAddress: fmt.Sprintf("127.0.0.1:%d", 9001+i),
			BootstrapAddresses: []string{
				bc.ListenAddress,
			},
			Listeners: 1,
		}

		dht, err := New(c)
		require.Nil(t, err)
		defer dht.Close()
	}

	// create a channel to handle our callback in a blocking way
	ch := make(chan error, 1)

	// attempt to store some data
	key := randomID()
	value := randomID()

	bdht.Store(key, value, time.Hour, func(err error) {
		ch <- err
	})

	require.Nil(t, <-ch)

	var rv []byte

	bdht.Find(key, func(v []byte, err error) {
		rv = make([]byte, len(v))
		copy(rv, v)
		ch <- err
	})

	require.Nil(t, <-ch)
	assert.Equal(t, value, rv)
}

func TestDHTClusterStoreFindMultiple(t *testing.T) {
	bc := &Config{
		LocalID:       randomID(),
		ListenAddress: "127.0.0.1:9000",
	}

	// create a new bootstrap node
	bdht, err := New(bc)
	require.Nil(t, err)
	defer bdht.Close()

	// wait some time for the listeners to start
	time.Sleep(time.Millisecond * 200)

	// add some nodes to the network
	for i := 0; i < 2; i++ {
		c := &Config{
			LocalID:       randomID(),
			ListenAddress: fmt.Sprintf("127.0.0.1:%d", 9001+i),
			BootstrapAddresses: []string{
				bc.ListenAddress,
			},
			Listeners: 1,
		}

		dht, err := New(c)
		require.Nil(t, err)
		defer dht.Close()
	}

	// create a channel to handle our callback in a blocking way
	ch := make(chan error, 1)

	// attempt to store some values to the same key
	key := randomID()

	values := make([][]byte, 10)

	for i := 0; i < len(values); i++ {
		values[i] = randomID()

		bdht.Store(key, values[i], time.Hour, func(err error) {
			ch <- err
		})

		require.Nil(t, <-ch)
	}

	type resp struct {
		data []byte
		err  error
	}

	// create a channel for our query responses
	ch2 := make(chan resp, 10)

	bdht.Find(key, func(v []byte, err error) {
		rv := make([]byte, len(v))
		copy(rv, v)
		ch2 <- resp{data: rv, err: err}
	})

	for i := 0; i < len(values); i++ {
		r := <-ch2
		require.Nil(t, r.err)
		assert.Equal(t, values[i], r.data)
	}
}

func TestDHTClusterStoreFindFromTimestamp(t *testing.T) {
	bc := &Config{
		LocalID:       randomID(),
		ListenAddress: "127.0.0.1:9000",
		Listeners:     1,
	}

	// create a new bootstrap node
	bdht, err := New(bc)
	require.Nil(t, err)
	defer bdht.Close()

	// wait some time for the listeners to start
	time.Sleep(time.Millisecond * 200)

	// add some nodes to the network
	for i := 0; i < 20; i++ {
		c := &Config{
			LocalID:       randomID(),
			ListenAddress: fmt.Sprintf("127.0.0.1:%d", 9001+i),
			BootstrapAddresses: []string{
				bc.ListenAddress,
			},
			Listeners: 1,
		}

		dht, err := New(c)
		require.Nil(t, err)
		defer dht.Close()
	}

	key := randomID()
	values := make([][]byte, 10)

	// create a channel to handle our callback in a blocking way
	ch := make(chan error, 1)

	for i := 0; i < 10; i++ {
		values[i] = randomID()

		bdht.Store(key, values[i], time.Hour, func(err error) {
			ch <- err
		})

		require.Nil(t, <-ch)
	}

	// wait for 1 second, as that's the smallest granularity we can query with (unix seconds)
	time.Sleep(time.Second)

	var result []byte

	from := time.Now()
	value := randomID()

	// find a value after we created the others
	bdht.Find(key, func(v []byte, err error) {
		result = make([]byte, len(v))
		copy(result, v)
		ch <- err
	}, ValuesFrom(from))

	// expect not found
	require.NotNil(t, <-ch)
	assert.Len(t, result, 0)

	// create a new value
	bdht.Store(key, value, time.Hour, func(err error) {
		ch <- err
	})

	require.Nil(t, <-ch)

	bdht.Find(key, func(v []byte, err error) {
		result = make([]byte, len(v))
		copy(result, v)
		ch <- err
	}, ValuesFrom(from))

	// expect not found
	// expect found
	require.Nil(t, <-ch)
	assert.Equal(t, value, result)
}

func TestDHTClusterStoreFindNonExistent(t *testing.T) {
	bc := &Config{
		LocalID:       randomID(),
		ListenAddress: "127.0.0.1:9000",
		Listeners:     1,
	}

	// create a new bootstrap node
	bdht, err := New(bc)
	require.Nil(t, err)
	defer bdht.Close()

	// wait some time for the listeners to start
	time.Sleep(time.Millisecond * 200)

	// add some nodes to the network
	for i := 0; i < 20; i++ {
		c := &Config{
			LocalID:       randomID(),
			ListenAddress: fmt.Sprintf("127.0.0.1:%d", 9001+i),
			BootstrapAddresses: []string{
				bc.ListenAddress,
			},
			Listeners: 1,
		}

		dht, err := New(c)
		require.Nil(t, err)
		defer dht.Close()
	}

	// create a channel to handle our callback in a blocking way
	ch := make(chan error, 1)

	key := randomID()

	bdht.Find(key, func(v []byte, err error) {
		ch <- err
	})

	require.NotNil(t, <-ch)
}

func TestDHTClusterNodeJoin(t *testing.T) {
	bc := &Config{
		LocalID:       randomID(),
		ListenAddress: "127.0.0.1:9000",
		Listeners:     1,
	}

	// create a new bootstrap node
	bdht, err := New(bc)
	require.Nil(t, err)
	defer bdht.Close()

	ch := make(chan error, 1)
	keys := make([][]byte, 1000)

	transferred := make(chan []byte, 1000)
	transferrable := make(map[uint64]struct{})

	// create config for a new node, but don't join yet
	c := &Config{
		LocalID:       randomID(),
		ListenAddress: "127.0.0.1:9001",
		BootstrapAddresses: []string{
			bc.ListenAddress,
		},
		Listeners: 1,
		Storage: newTestStorage(func(v *Value) {
			transferred <- v.Key
		}),
	}

	// store some keys to the bootstrap node
	for i := 0; i < 1000; i++ {
		k := randomID()

		bdht.Store(k, k, time.Hour, func(err error) {
			ch <- err
		})

		require.Nil(t, <-ch)

		keys[i] = k

		/*
			d1 := distance(bc.LocalID, k)
			d2 := distance(c.LocalID, k)

			if d2 > d1 {
				transferrable[testHash(k)] = struct{}{}
			}
		*/

		transferrable[testHash(k)] = struct{}{}
	}

	// wait some time for the listeners to start
	time.Sleep(time.Millisecond * 200)

	// add a new node to the network
	dht, err := New(c)
	require.Nil(t, err)
	defer dht.Close()

	for i := 0; i < len(transferrable); i++ {
		select {
		case k := <-transferred:
			_, ok := transferrable[testHash(k)]
			assert.True(t, ok)
		case <-time.After(time.Second * 5):
			require.Fail(t, "timed out")
		}
	}

	// TODO add test case replicating when network size is > K
}

func TestDHTClusterNodeJoinLeave(t *testing.T) {
	bc := &Config{
		LocalID:       randomID(),
		ListenAddress: "127.0.0.1:9000",
		Listeners:     2,
	}

	// create a new bootstrap node
	bdht, err := New(bc)
	require.Nil(t, err)
	defer bdht.Close()

	ch := make(chan error, 1)
	keys := make([][]byte, 100)

	// store some keys to the bootstrap node
	for i := 0; i < len(keys); i++ {
		k := randomID()

		bdht.Store(k, k, time.Hour, func(err error) {
			ch <- err
		})

		require.Nil(t, <-ch)

		keys[i] = k
	}

	// make this channel big enough that it can never
	// block if all keys are transferred to all nodes
	transferred := make(chan interface{}, 20000)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		// wait some time for the nodes to start and for values
		// to be replicated to the joining nodes
		// TODO : improve this
		for {
			if wait(transferred, time.Second*10) != nil {
				wg.Done()
				return
			}
		}
	}()

	for i := 0; i < 20; i++ {
		// start 20 nodes
		c := &Config{
			LocalID:       randomID(),
			ListenAddress: fmt.Sprintf("127.0.0.1:%d", 9001+i),
			BootstrapAddresses: []string{
				bc.ListenAddress,
			},
			Listeners: 2,
			Timeout:   time.Millisecond * 100,
			Storage: newTestStorage(func(value *Value) {
				transferred <- value.Key
			}),
		}

		// add a new node to the network
		dht, err := New(c)
		require.Nil(t, err)
		defer dht.Close()

		time.Sleep(time.Millisecond * 10)
	}

	wg.Wait()

	c := &Config{
		LocalID:       randomID(),
		ListenAddress: "127.0.0.1:10000",
		BootstrapAddresses: []string{
			bc.ListenAddress,
		},
		Listeners: 2,
		Timeout:   time.Millisecond * 100,
	}

	// add a new node to the network
	dht, err := New(c)
	require.Nil(t, err)
	defer dht.Close()

	time.Sleep(time.Millisecond * 10)

	// stop the original bootstrap node
	bdht.Close()

	var missing int

	// search for the original keys that were added to the network
	for _, k := range keys {

		dht.Find(k, func(v []byte, err error) {
			ch <- err
		})

		// require.Nil(t, <-ch)
		if <-ch != nil {
			missing++
		}
	}

	// TODO : compare with keys transferred to ensure the number
	// of keys lost matches what was not transferred
	assert.Equal(t, missing, 0)
}

func BenchmarkDHTLocalStore(b *testing.B) {
	c := &Config{
		LocalID:       randomID(),
		ListenAddress: "127.0.0.1:9000",
	}

	// create a new dht with no nodes
	dht, err := New(c)
	require.Nil(b, err)
	defer dht.Close()

	// add itself to it's routing table
	addr, err := net.ResolveUDPAddr("udp", c.ListenAddress)
	require.Nil(b, err)

	dht.routing.insert(c.LocalID, addr)

	// wait some time for the listeners to start
	time.Sleep(time.Millisecond * 200)

	// create a channel to handle our callback in a blocking way
	b.ResetTimer()
	b.ReportAllocs()

	// test with multiple store requests in parallel
	b.RunParallel(func(pb *testing.PB) {
		ch := make(chan error, 1)

		key := randomID()
		value := randomID()

		for pb.Next() {
			// attempt to store some data

			dht.Store(key, value, time.Hour, func(err error) {
				ch <- err
			})

			<-ch
		}
	})
}

func BenchmarkDHTClusterStore(b *testing.B) {
	dhts := make([]*DHT, 100)

	for i := 0; i < 100; i++ {
		c := &Config{
			LocalID:       randomID(),
			ListenAddress: fmt.Sprintf("127.0.0.1:%d", 9000+i),
			Listeners:     1,
		}

		if i > 0 {
			c.BootstrapAddresses = []string{
				dhts[0].config.ListenAddress,
			}
		}

		// create a new dht with no nodes
		dht, err := New(c)
		require.Nil(b, err)
		defer dht.Close()

		dhts[i] = dht
	}

	// wait some time for the listeners to start
	// time.Sleep(time.Millisecond * 200)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		key := randomID()
		value := randomID()

		// responses chan
		ch := make(chan error, 50000)

		for pb.Next() {
			// attempt to store some data
			dhts[0].Store(key, value, time.Hour, func(err error) {
				ch <- err
			})

			<-ch
		}
	})
}

func BenchmarkDHTClusterStoreLargeValue(b *testing.B) {
	dhts := make([]*DHT, 100)

	for i := 0; i < 100; i++ {
		c := &Config{
			LocalID:       randomID(),
			ListenAddress: fmt.Sprintf("127.0.0.1:%d", 9000+i),
			Listeners:     1,
		}

		if i > 0 {
			c.BootstrapAddresses = []string{
				dhts[0].config.ListenAddress,
			}
		}

		// create a new dht with no nodes
		dht, err := New(c)
		require.Nil(b, err)
		defer dht.Close()

		dhts[i] = dht
	}

	// wait some time for the listeners to start
	time.Sleep(time.Millisecond * 200)

	b.ResetTimer()
	b.ReportAllocs()

	// test with multiple store requests in parallel
	b.RunParallel(func(pb *testing.PB) {
		ch := make(chan error, 1)

		key := randomID()
		value := make([]byte, 2048)
		rand.Read(value)

		for pb.Next() {
			// attempt to store some data
			dhts[0].Store(key, value, time.Hour, func(err error) {
				ch <- err
			})

			<-ch
		}
	})
}

func BenchmarkDHTLocalFind(b *testing.B) {
	c := &Config{
		LocalID:       randomID(),
		ListenAddress: "127.0.0.1:9000",
	}

	// create a new dht with no nodes
	dht, err := New(c)
	require.Nil(b, err)
	defer dht.Close()

	// add itself to it's routing table
	addr, err := net.ResolveUDPAddr("udp", c.ListenAddress)
	require.Nil(b, err)

	dht.routing.insert(c.LocalID, addr)

	// wait some time for the listeners to start
	time.Sleep(time.Millisecond * 200)

	// create a channel to handle our callback in a blocking way
	b.ResetTimer()
	b.ReportAllocs()

	ch := make(chan error, 1)

	// attempt to store some data
	key := randomID()
	value := randomID()

	dht.Store(key, value, time.Hour, func(err error) {
		ch <- err
	})

	err = <-ch
	require.Nil(b, err)

	// test with multiple find requests in parallel
	// these requests are synchronous, you could maybe
	// get some more performance running them async
	b.RunParallel(func(pb *testing.PB) {
		ch := make(chan error, 1)

		for pb.Next() {
			dht.Find(key, func(value []byte, err error) {
				ch <- err
			})

			<-ch
		}
	})
}

func BenchmarkDHTClusterFind(b *testing.B) {
	dhts := make([]*DHT, 100)

	for i := 0; i < 100; i++ {
		c := &Config{
			LocalID:       randomID(),
			ListenAddress: fmt.Sprintf("127.0.0.1:%d", 9000+i),
			Listeners:     1,
		}

		if i > 0 {
			c.BootstrapAddresses = []string{
				dhts[0].config.ListenAddress,
			}
		}

		// create a new dht with no nodes
		dht, err := New(c)
		require.Nil(b, err)
		defer dht.Close()

		dhts[i] = dht
	}

	// wait some time for the listeners to start
	time.Sleep(time.Millisecond * 200)

	ch := make(chan error, 1)

	keys := make([][]byte, 1000)

	// store some data on the network
	for i := 0; i < 1000; i++ {
		r := randomID()

		// attempt to store some data
		dhts[0].Store(r, r, time.Hour, func(err error) {
			ch <- err
		})

		<-ch

		keys[i] = r
	}

	b.ResetTimer()
	b.ReportAllocs()

	var pos int32

	// test with multiple store requests in parallel
	b.RunParallel(func(pb *testing.PB) {
		ch := make(chan error, 1)

		for pb.Next() {
			// attempt to store some data
			k := keys[atomic.AddInt32(&pos, 1)%1000]

			dhts[0].Find(k, func(value []byte, err error) {
				ch <- err
			})

			<-ch
		}
	})
}

func BenchmarkDHTClusterFindLargeValue(b *testing.B) {
	dhts := make([]*DHT, 100)

	for i := 0; i < 100; i++ {
		c := &Config{
			LocalID:       randomID(),
			ListenAddress: fmt.Sprintf("127.0.0.1:%d", 9000+i),
			Listeners:     1,
		}

		if i > 0 {
			c.BootstrapAddresses = []string{
				dhts[0].config.ListenAddress,
			}
		}

		// create a new dht with no nodes
		dht, err := New(c)
		require.Nil(b, err)
		defer dht.Close()

		dhts[i] = dht
	}

	// wait some time for the listeners to start
	time.Sleep(time.Millisecond * 200)

	ch := make(chan error, 1)

	keys := make([][]byte, 1000)

	// store some data on the network
	for i := 0; i < 1000; i++ {
		k := randomID()
		v := make([]byte, 2048)
		rand.Read(v)

		// attempt to store some data
		dhts[0].Store(k, v, time.Hour, func(err error) {
			ch <- err
		})

		<-ch

		keys[i] = k
	}

	b.ResetTimer()
	b.ReportAllocs()

	var pos int32

	// test with multiple store requests in parallel
	b.RunParallel(func(pb *testing.PB) {
		ch := make(chan error, 1)

		for pb.Next() {
			// attempt to store some data
			k := keys[atomic.AddInt32(&pos, 1)%1000]

			dhts[0].Find(k, func(value []byte, err error) {
				ch <- err
			})

			<-ch
		}
	})
}

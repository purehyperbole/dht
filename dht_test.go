package dht

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDHTStoreFindLocal(t *testing.T) {
	c := &Config{
		LocalID:       randomID(),
		ListenAddress: "127.0.0.1:9000",
	}

	// create a new dht with no nodes
	dht, err := New(c)
	require.Nil(t, err)
	defer dht.Close()

	// add itself to it's routing table
	addr, err := net.ResolveUDPAddr("udp", c.ListenAddress)
	require.Nil(t, err)

	dht.routing.insert(&node{
		id:      c.LocalID,
		address: addr,
	})

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

func BenchmarkDHTStoreLocal(b *testing.B) {
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

	dht.routing.insert(&node{
		id:      c.LocalID,
		address: addr,
	})

	// wait some time for the listeners to start
	time.Sleep(time.Millisecond * 200)

	// create a channel to handle our callback in a blocking way
	b.ResetTimer()
	b.ReportAllocs()

	// test with multiple store requests in parallel
	b.RunParallel(func(pb *testing.PB) {
		ch := make(chan error, 1)

		for pb.Next() {
			// attempt to store some data
			key := randomID()
			value := randomID()

			dht.Store(key, value, time.Hour, func(err error) {
				ch <- err
			})

			err = <-ch
		}
	})
}

func BenchmarkDHTFindLocal(b *testing.B) {
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

	dht.routing.insert(&node{
		id:      c.LocalID,
		address: addr,
	})

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

	// test with multiple find requests in parallel
	b.RunParallel(func(pb *testing.PB) {
		ch := make(chan error, 1)

		for pb.Next() {
			dht.Find(key, func(value []byte, err error) {
				ch <- err
			})

			err = <-ch
		}
	})
}

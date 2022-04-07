package dht

import (
	"crypto/rand"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestDHTStoreFindLocal(t *testing.T) {
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

func TestDHTStoreFindCluster(t *testing.T) {
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
	for i := 0; i < 100; i++ {
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

			err = <-ch
		}
	})
}

func BenchmarkDHTStoreCluster(b *testing.B) {
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

	// test with multiple store requests in parallel
	b.RunParallel(func(pb *testing.PB) {
		ch := make(chan error, 1)

		key := randomID()
		value := randomID()

		for pb.Next() {
			// attempt to store some data
			dhts[0].Store(key, value, time.Hour, func(err error) {
				ch <- err
			})

			<-ch
		}
	})
}

func BenchmarkDHTStoreClusterLargeValue(b *testing.B) {
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

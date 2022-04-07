package dht

import (
	"fmt"
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
	value := []byte("HELLO") // randomID()

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
	fmt.Println(string(value), string(rv))

	assert.Equal(t, value, rv)
}

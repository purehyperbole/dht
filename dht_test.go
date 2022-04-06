package dht

import (
	"net"
	"testing"
	"time"

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

	// create a channel to handle our callback in a blocking way
	ch := make(chan error, 1)

	// attempt to store some data
	key := randomID()
	value := randomID()

	dht.Store(key, value, time.Hour, func(err error) {
		ch <- err
	})

	require.Nil(t, <-ch)
}

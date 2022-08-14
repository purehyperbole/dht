package dht

import (
	"crypto/rand"
	"net"
	"time"
)

// node represents a node on the network
type node struct {
	// the id of the node (default to 160 bits/20 bytes)
	id []byte
	// the udp address of the node
	address *net.UDPAddr
	// the last time an event was received from this node
	seen time.Time
	// the number of expected responses we are waiting on
	pending int
}

func randomID() []byte {
	id := make([]byte, KEY_BYTES)
	rand.Read(id)
	return id
}

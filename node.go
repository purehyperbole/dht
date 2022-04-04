package dht

import (
	"crypto/rand"
	"math/big"
	"net"
	"time"
)

// node represents a node on the network
type node struct {
	// the 160 bit id of the node
	id []byte
	// the udp address of the node
	address *net.UDPAddr
	// the last time an event was received from this node
	seen time.Time
	// the number of expected responses we are waiting on
	pending int
}

// newNode creates a new node from it's id and address
func newNode(id []byte, address *net.UDPAddr) *node {
	bi := big.NewInt(0)
	bi.SetBytes(id)

	return &node{
		id:      id,
		address: address,
	}
}

// ping sends the rpc ping command
func (n *node) ping() error {
	return nil
}

// store sends the rpc store command
func (n *node) store(key, value []byte) error {
	return nil
}

// findnode sends the rpc find node command
func (n *node) findNode(nodeID []byte) ([]*node, error) {
	return nil, nil
}

// findValue sends the rpc find value command
func (n *node) findValue(key []byte) ([]*node, []byte, error) {
	return nil, nil, nil
}

func randomID() []byte {
	id := make([]byte, 20)
	rand.Read(id)
	return id
}

package dht

import (
	"net"
)

// node represents a node on the network
type node struct {
	ID      []byte
	Address *net.UDPAddr
}

// newNode creates a new node from it's id and address
func newNode(id []byte, address *net.UDPAddr) *node {
	return &node{
		ID:      id,
		Address: address,
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

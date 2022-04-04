package dht

import (
	"math/bits"
)

const (
	KEY_BITS = 160
)

// routing table stores buckets of every known node on the network
type routingTable struct {
	localNode *node
	// the number of active buckets
	total int
	// buckets of nodes active in the routing table
	buckets []bucket
}

// newRoutingTable creates a new routing table
func newRoutingTable(localNode *node) *routingTable {
	buckets := make([]bucket, KEY_BITS)

	for i := range buckets {
		buckets[i].nodes = make([]*node, 20)
	}

	return &routingTable{
		localNode: localNode,
		buckets:   buckets,
	}
}

// insert a node to its corresponding bucket
func (t *routingTable) insert(n *node) {
	t.buckets[bucketID(t.localNode.id, n.id)].insert(n)
}

// finds the closest known node for a given key
func (t *routingTable) findClosest(id []byte) *node {
	offset := bucketID(t.localNode.id, id)

	// scan outwardly from our selected bucket until we find a
	// node that is close to the target key
	// really not sure if this will work as expected...
	for i := 0; i < 160; i++ {
		var cd int
		var cn *node

		if offset > 0 && offset < 160 {
			t.buckets[offset].iterate(func(n *node) {
				// find a node which has the most matching bits
				nd := distance(n.id, id)
				if cd == 0 || nd > cd {
					cd = nd
					cn = n
				}
			})

			if cn != nil {
				return cn
			}
		}

		if i%2 == 0 {
			offset = offset + i + 1
		} else {
			offset = offset - i - 1
		}
	}

	return nil
}

// bucketID gets the correct bucket id for a given node, based on it's xor distance from our node
func bucketID(localID, targetID []byte) int {
	pfx := distance(localID, targetID)

	d := (KEY_BITS - pfx)

	if d == 0 {
		return d
	}

	return d - 1
}

func distance(localID, targetID []byte) int {
	var pfx int

	// xor each byte and check for the number of 0 least significant bits
	for i := 0; i < KEY_BITS/8; i++ {
		d := localID[i] ^ targetID[i]

		if d == 0 {
			// byte is all 0's, so we add all bits
			pfx = pfx + 8
		} else {
			// there are some differences with this byte, so get the number
			// of leading zero bits and add them to the prefix
			pfx = pfx + bits.LeadingZeros8(d)
			break
		}
	}

	return pfx
}

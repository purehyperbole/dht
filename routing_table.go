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

func (t *routingTable) insert(n *node) {
	t.buckets[bucketID(t.localNode.id, n.id)].insert(n)
}

// bucketID gets the correct bucket id for a given node, based on it's xor distance from our node
func bucketID(localNodeID, remoteNodeID []byte) int {
	var pfx int

	// xor each byte and check for the number of 0 least significant bits
	for i := 0; i < KEY_BITS/8; i++ {
		d := localNodeID[i] ^ remoteNodeID[i]

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

	d := (KEY_BITS - pfx)

	if d == 0 {
		return d
	}

	return d - 1
}

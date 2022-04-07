package dht

import (
	"math/bits"
	"net"
	"sort"
)

const (
	// K number of nodes in a bucket
	K = 20
	// KEY_BITS number of bits in a key
	KEY_BITS = 160
	// KEY_BYTES number of bytes in a key
	KEY_BYTES = KEY_BITS / 8
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
		buckets[i].nodes = make([]*node, K)
	}

	return &routingTable{
		localNode: localNode,
		buckets:   buckets,
	}
}

// insert a node to its corresponding bucket
func (t *routingTable) insert(id []byte, address *net.UDPAddr) {
	t.buckets[bucketID(t.localNode.id, id)].insert(id, address)
}

// updates the timestamp of a node to seen
// returns true if the node exists and false
// if the node needs to be inserted into the
// routing table
func (t *routingTable) seen(id []byte) bool {
	return t.buckets[bucketID(t.localNode.id, id)].seen(id)
}

// remove the node from the routing table
func (t *routingTable) remove(id []byte) {
	t.buckets[bucketID(t.localNode.id, id)].remove(id, true)
}

// finds the closest known node for a given key
func (t *routingTable) closest(id []byte) *node {
	offset := bucketID(t.localNode.id, id)

	// scan outwardly from our selected bucket until we find a
	// node that is close to the target key
	var i int
	var scanned int

	for {
		var cd int
		var cn *node

		if offset > -1 && offset < KEY_BITS {
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

			scanned++
		}

		if scanned >= KEY_BITS {
			break
		}

		if i%2 == 0 {
			offset = offset + i + 1
		} else {
			offset = offset - i - 1
		}

		i++
	}

	return nil
}

// finds the closest known nodes for a given key
func (t *routingTable) closestN(id []byte, count int) []*node {
	offset := bucketID(t.localNode.id, id)

	var nodes []*node

	// scan outwardly from our selected bucket until we find a
	// node that is close to the target key
	var i int
	var scanned int

	for {
		if offset > -1 && offset < KEY_BITS {
			t.buckets[offset].iterate(func(n *node) {
				nodes = append(nodes, n)
			})

			if len(nodes) >= count {
				break
			}

			scanned++
		}

		if scanned >= KEY_BITS {
			break
		}

		if i%2 == 0 {
			offset = offset + i + 1
		} else {
			offset = offset - i - 1
		}

		i++
	}

	sort.Slice(nodes, func(i, j int) bool {
		idst := distance(nodes[i].id, id)
		jdst := distance(nodes[j].id, id)
		return idst > jdst
	})

	if len(nodes) < count {
		return nodes
	}

	return nodes[:count]
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
	for i := 0; i < KEY_BYTES; i++ {
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

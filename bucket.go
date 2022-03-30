package dht

import "bytes"

type bucket struct {
	size       int
	neighbours []*node
}

// inserts a node into the bucket. if the bucket
// is full, it returns the least recently seen node.
// this node needs to be pinged and if unresponsive,
// can be evicted from the bucket
func (b *bucket) insert(n *node) *node {

	return nil
}

// returns the index of a node. if it does not exist
// it returns -1
func (b *bucket) nodeIndex(n *node) int {
	for i := 0; i < b.size; i++ {
		if bytes.Equal(b.neighbours[i].ID, n.ID) {
			return i
		}
	}

	return -1
}

func (b *bucket) full() bool {
	return b.size == 20
}

func (b *bucket) clone() *bucket {
	neighbours := make([]*node, 20)
	copy(neighbours, b.neighbours)

	return &bucket{
		size:       b.size,
		neighbours: b.neighbours,
	}
}

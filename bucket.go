package dht

import (
	"bytes"
	"sync"
	"time"
)

type bucket struct {
	// the number of nodes in the bucket, excluding the promotion cache
	size int
	// the amout of time before a node is considered stale
	expiry time.Duration
	// nodes holds all active nodes on the network
	nodes []*node
	// cache holds all nodes that could be promoted to the bucket when
	// other nodes expire
	cache []*node
	mu    sync.Mutex
}

// inserts a node into the bucket. if the bucket
// is full, it returns the least recently seen node.
// this node needs to be pinged and if unresponsive,
// can be evicted from the bucket
func (b *bucket) insert(n *node) *node {
	b.mu.Lock()
	defer b.mu.Unlock()

	// try to remove the node. If it exists in the bucket,
	// then update it and add it to the end of the list
	rn := b.remove(n.id)
	if rn != nil {
		rn.seen = time.Now()
		b.nodes[b.size] = rn
		b.size++

		return nil
	}

	// if the bucket is not full, add the new node to the end
	if !b.full() {
		n.seen = time.Now()
		b.nodes[b.size] = n
		b.size++

		return nil
	}

	var si int
	var stale *node

	now := time.Now()

	// check for any stale entries
	for i := 0; i < b.size; i++ {
		en := b.nodes[i]

		if now.After(n.seen.Add(b.expiry)) {
			if stale == nil && en.pending > 1 {
				stale = en
				si = i
			} else if stale != nil && en.pending > stale.pending {
				stale = en
				si = i
			}
		}
	}

	// delete the stalest entry
	if stale != nil {
		copy(b.nodes[si:], b.nodes[si+1:])
		b.nodes[b.size] = n
		return nil
	}

	// if there's no space in the bucket, we add the node to the promotion cache
	// so it can be added to the main node list when other nodes expire
	b.stash(n)

	return nil
}

// finds the closest known nodes for a given key
func (b *bucket) findClosest(id []byte, count int) []*node {

	return nil
}

// gets a node by its id
func (b *bucket) get(nodeID []byte) *node {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i := 0; i < b.size; i++ {
		if bytes.Equal(b.nodes[i].id, nodeID) {
			return b.nodes[i]
		}
	}

	return nil
}

// sets a node as recently seen by updating it's seen timestamp
// if it still exists in the bucket. this is called when a node has
// responded to a request
func (b *bucket) seen(nodeID []byte) {
	n := b.get(nodeID)
	if n != nil {
		n.seen = time.Now()
	}
}

// removes a node and returns it if it exists
func (b *bucket) remove(nodeID []byte) *node {
	for i := b.size - 1; i >= 0; i-- {
		if bytes.Equal(b.nodes[i].id, nodeID) {
			r := b.nodes[i]

			copy(b.nodes[i:], b.nodes[i+1:])
			b.size--

			return r
		}
	}

	// TODO : promote a node from the promotion cache

	return nil
}

// stash stashes a node in the promotion cache
func (b *bucket) stash(n *node) {
	for i := range b.cache {
		if bytes.Equal(b.cache[i].id, n.id) {
			b.cache[i].seen = time.Now()
			return
		}
	}

	// TODO : restrict the size of the cache and
	// evict the oldest members of this cache before
	// adding any new items

	b.cache = append(b.cache, n)
}

func (b *bucket) full() bool {
	return b.size == 20
}

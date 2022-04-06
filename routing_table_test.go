package dht

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoutingTableFindNearest(t *testing.T) {
	rt := newRoutingTable(&node{
		id: randomID(),
	})

	// insert 10000 nodes into the routing table
	for i := 0; i < 10000; i++ {
		rt.insert(&node{
			id: randomID(),
		})
	}

	// generate a random target key we want to look up
	target := randomID()

	n := rt.closest(target)
	require.NotNil(t, n)

	// check all nodes to ensure we actually found the closest node
	var nodes []*node

	for i := range rt.buckets {
		rt.buckets[i].iterate(func(nd *node) {
			nodes = append(nodes, nd)
		})
	}

	sort.Slice(nodes, func(i, j int) bool {
		d1 := distance(nodes[i].id, target)
		d2 := distance(nodes[j].id, target)

		// we're sorting for the closest distance,
		// which is actually the greatest number of
		// matching bits, hence why we compare with >
		return d1 > d2
	})

	assert.Equal(t, n.id, nodes[0].id)
}

func TestRoutingTableFindNearestN(t *testing.T) {
	rt := newRoutingTable(&node{
		id: randomID(),
	})

	// generate a random target key we want to look up
	target := randomID()

	// try to find nodes on an empty table
	ns := rt.closestN(target, 3)
	require.Len(t, ns, 0)

	// insert 10000 nodes into the routing table
	for i := 0; i < 10000; i++ {
		rt.insert(&node{
			id: randomID(),
		})
	}

	// try to find closest nodes on a populated table
	ns = rt.closestN(target, 3)
	require.Len(t, ns, 3)

	assert.Equal(t, rt.closest(target).id, ns[0].id)
}

func BenchmarkRoutingTableFindNearest(b *testing.B) {
	rt := newRoutingTable(&node{
		id: randomID(),
	})

	// insert 10000 nodes into the routing table
	for i := 0; i < 10000; i++ {
		rt.insert(&node{
			id: randomID(),
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		target := randomID()

		rt.closest(target)
	}
}

func BenchmarkRoutingTableFindNearestN(b *testing.B) {
	rt := newRoutingTable(&node{
		id: randomID(),
	})

	// insert 10000 nodes into the routing table
	for i := 0; i < 10000; i++ {
		rt.insert(&node{
			id: randomID(),
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		target := randomID()

		rt.closestN(target, 3)
	}
}

func BenchmarkRoutingTableInsert(b *testing.B) {
	rt := newRoutingTable(&node{
		id: randomID(),
	})

	nodes := make([]*node, 10000)

	// preallocate 10,000 nodes
	// should simulate seeing the same
	for i := 0; i < 10000; i++ {
		nodes[i] = &node{
			id: randomID(),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rt.insert(nodes[i%10000])
	}
}

func BenchmarkRoutingTableSeen(b *testing.B) {
	rt := newRoutingTable(&node{
		id: randomID(),
	})

	nodes := make([]*node, 10000)

	// preallocate 10,000 nodes
	// should simulate seeing the same
	for i := 0; i < 10000; i++ {
		nodes[i] = &node{
			id: randomID(),
		}

		rt.insert(nodes[i])
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rt.seen(nodes[i%10000].id)
	}
}

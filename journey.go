package dht

import (
	"bytes"
	"hash/maphash"
	"sort"
	"sync"
)

// journey tracks the optimum K routes
// that have not been visited before
type journey struct {
	// address to skip, as its this node
	source []byte
	// the target we want to arrive at
	destination []byte
	// a set of nodes we have already visited
	visited map[uint64]struct{}
	// hasher for our list of destinations
	hasher maphash.Hash
	// potential routes that we can send requests to
	nodes []*node
	// the computed distances of each of the available routes
	distances []int
	// the number of routes/nodes in our list
	routes int
	// the remaining iterations we have to make
	remaining int
	// the number of inflight requests used to track when to return a timeout to the user
	inflight int
	// the journey has been completed
	completed bool
	mu        sync.Mutex
}

func newJourney(source, destination []byte, iterations int) *journey {
	var hasher maphash.Hash
	hasher.SetSeed(maphash.MakeSeed())

	return &journey{
		source:      source,
		destination: destination,
		visited:     make(map[uint64]struct{}),
		hasher:      hasher,
		nodes:       make([]*node, K),
		distances:   make([]int, K),
		remaining:   iterations,
	}
}

// adds routes to our list of nodes. if they have
// been visited before on this journey, they will
// be skipped
func (j *journey) add(nodes []*node) {
	j.mu.Lock()

	for _, n := range nodes {
		// don't add node if it exists, or it's this node
		if bytes.Equal(n.id, j.source) {
			continue
		}

		j.hasher.Reset()
		j.hasher.Write(n.id)
		k := j.hasher.Sum64()

		// calculate the distance to the current node
		d := distance(n.id, j.destination)

		// if we have visited this node before, skip it
		_, ok := j.visited[k]
		if ok {
			continue
		}

		j.visited[k] = struct{}{}

		//fmt.Println("journey: adding", n.address.String())

		// if the list isn't full, add it to the list
		if j.routes < K {
			j.nodes[j.routes] = n
			j.distances[j.routes] = d
			j.routes++
			continue
		}

		// the list is full, so select the first node
		// we find that is worse than us
		for i := 0; i < K; i++ {
			if j.distances[i] < d {
				// remove this from our set of nodes
				j.hasher.Reset()
				j.hasher.Write(j.nodes[i].id)
				k := j.hasher.Sum64()

				delete(j.visited, k)

				// there are less matching bits to the target
				// so we can replace this completely
				j.nodes[i] = n
				j.distances[i] = d

				break
			}
		}
	}

	j.mu.Unlock()
}

// returns the next set of viable routes, returns nil if there are
// no more left, or if the
func (j *journey) next(count int) []*node {
	j.mu.Lock()
	defer j.mu.Unlock()

	// if we've reached the maximum iterations or there are no more
	// routes left, dont provide any more routes. If the journey
	// has been completed and the destination reched, then don't
	// return more routes
	if j.remaining == 0 || j.routes == 0 || j.completed {
		return nil
	}

	j.remaining--

	available := count

	if j.routes < count {
		available = j.routes
	}

	j.inflight = j.inflight + available

	// sort to find the best possible routes
	sort.Sort(j)

	// create a new array that we can copy our nodes into
	next := make([]*node, available)
	copy(next, j.nodes[:available])

	// remove the nodes/distances from our list of routes
	copy(j.nodes, j.nodes[available:])
	copy(j.distances, j.distances[available:])
	j.routes = j.routes - available

	// log.Println("sending nodes:", available, "inflight:", j.inflight, "iterations:", K-j.remaining, "routes:", j.routes)

	return next
}

// marks the journey as completed
func (j *journey) finish(force bool) bool {
	j.mu.Lock()
	defer j.mu.Unlock()

	if force {
		if j.completed {
			return false
		}
	} else {
		if j.completed || j.inflight > 0 {
			return false
		}
	}

	j.completed = true

	return true
}

// responseReceived marks an inflight request as responded to.
// returns the journeys completion status and if we should return
// an error to the user
func (j *journey) responseReceived() (bool, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()

	// inf := j.inflight

	if j.inflight > 0 {
		j.inflight--
	}

	// log.Println("response completed:", j.completed, "inflight (old):", inf, "inflight (new):", j.inflight, "routes:", j.routes)

	// if we've exhausted all routes and we're still not completed,
	// mark this journey as done for the next response we might receive
	return j.completed, j.inflight < 1 && j.routes < 1
}

/*
func (j *journey) has(n *node) bool {
	for i := 0; i < j.routes; i++ {
		if bytes.Equal(j.nodes[i].id, n.id) {
			return true
		}
	}
	return false
}
*/

// Returns the length of the available routes
func (j *journey) Len() int {
	return j.routes
}

// Swap swaps the available routes and their distances from the target/destination
func (j *journey) Swap(x, y int) {
	j.nodes[x], j.nodes[y] = j.nodes[y], j.nodes[x]
	j.distances[x], j.distances[y] = j.distances[y], j.distances[x]
}

// Less returns true if x distance is closer to the destination than y
func (j *journey) Less(x, y int) bool {
	return j.distances[x] > j.distances[y]
}

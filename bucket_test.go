package dht

import (
	"bytes"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBucketInsertAndGet(t *testing.T) {
	b := bucket{
		nodes:  make([]*node, 20),
		expiry: time.Minute,
	}

	ids := make([][]byte, 100)

	for i := 0; i < 100; i++ {
		id := make([]byte, 20)
		rand.Read(id)

		ids[i] = id
		b.insert(id, nil)
	}

	assert.Equal(t, 20, b.size)

	for i := range ids {
		if i >= 20 {
			var found bool

			for x := 0; x < b.size; x++ {
				if bytes.Equal(b.nodes[x].id, ids[i]) {
					found = true
					break
				}
			}

			assert.False(t, found)
		}
	}
}

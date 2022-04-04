package dht

import (
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
		b.insert(&node{id: id})
	}

	for i := range ids {
		if i >= 20 {
			assert.Nil(t, b.get(ids[i]))
		}
	}
}

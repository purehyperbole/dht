package dht

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"
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
		if b.get(ids[i]) == nil {
			fmt.Println(i, "is not in the bucket")
		}
	}
}

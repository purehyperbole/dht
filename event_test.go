package dht

import (
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
)

func TestEventStoreRequest(t *testing.T) {
	b := flatbuffers.NewBuilder(65535)

	eventStoreRequest(b, randomID(), randomID(), []*Value{
		{
			Key:   randomID(),
			Value: []byte{},
			TTL:   time.Second,
		},
	})
}

package dht

import (
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/purehyperbole/dht/protocol"
)

func eventPing(buf *flatbuffers.Builder, id, sender []byte) []byte {
	buf.Reset()

	eid := buf.CreateByteVector(id)
	snd := buf.CreateByteVector(sender)

	protocol.EventStart(buf)
	protocol.EventAddId(buf, eid)
	protocol.EventAddSender(buf, snd)
	protocol.EventAddEvent(buf, protocol.EventTypePING)
	protocol.EventAddResponse(buf, false)

	e := protocol.EventEnd(buf)

	buf.Finish(e)

	return buf.FinishedBytes()
}

func eventPong(buf *flatbuffers.Builder, id, sender []byte) []byte {
	buf.Reset()

	eid := buf.CreateByteVector(id)
	snd := buf.CreateByteVector(sender)

	protocol.EventStart(buf)
	protocol.EventAddId(buf, eid)
	protocol.EventAddSender(buf, snd)
	protocol.EventAddEvent(buf, protocol.EventTypePONG)
	protocol.EventAddResponse(buf, true)

	e := protocol.EventEnd(buf)

	buf.Finish(e)

	return buf.FinishedBytes()
}

func eventStoreRequest(buf *flatbuffers.Builder, id, sender []byte, values []*Value) []byte {
	buf.Reset()

	// construct the value vector
	vs := make([]flatbuffers.UOffsetT, len(values))

	for i, value := range values {
		k := buf.CreateByteVector(value.Key)
		v := buf.CreateByteVector(value.Value)

		protocol.ValueStart(buf)
		protocol.ValueAddKey(buf, k)
		protocol.ValueAddValue(buf, v)
		protocol.ValueAddTtl(buf, int64(value.TTL))
		vs[i] = protocol.ValueEnd(buf)
	}

	protocol.FindNodeStartNodesVector(buf, len(values))

	// prepend nodes to vector in reverse order
	for i := len(values) - 1; i >= 0; i-- {
		buf.PrependUOffsetT(vs[0])
	}

	vv := buf.EndVector(len(values))

	// construct the find node table
	protocol.StoreStart(buf)
	protocol.StoreAddValues(buf, vv)
	s := protocol.StoreEnd(buf)

	eid := buf.CreateByteVector(id)
	snd := buf.CreateByteVector(sender)

	protocol.EventStart(buf)
	protocol.EventAddId(buf, eid)
	protocol.EventAddSender(buf, snd)
	protocol.EventAddEvent(buf, protocol.EventTypeSTORE)
	protocol.EventAddResponse(buf, false)
	protocol.EventAddPayload(buf, s)

	e := protocol.EventEnd(buf)

	buf.Finish(e)

	return buf.FinishedBytes()
}

func eventStoreResponse(buf *flatbuffers.Builder, id, sender []byte) []byte {
	buf.Reset()

	eid := buf.CreateByteVector(id)
	snd := buf.CreateByteVector(sender)

	protocol.EventStart(buf)
	protocol.EventAddId(buf, eid)
	protocol.EventAddSender(buf, snd)
	protocol.EventAddEvent(buf, protocol.EventTypeSTORE)
	protocol.EventAddResponse(buf, true)

	e := protocol.EventEnd(buf)

	buf.Finish(e)

	return buf.FinishedBytes()
}

func eventFindNodeRequest(buf *flatbuffers.Builder, id, sender, key []byte) []byte {
	buf.Reset()

	k := buf.CreateByteVector(key)

	// construct the find node table
	protocol.FindNodeStart(buf)
	protocol.FindNodeAddKey(buf, k)
	fn := protocol.FindNodeEnd(buf)

	// construct the response event table
	eid := buf.CreateByteVector(id)
	snd := buf.CreateByteVector(sender)

	protocol.EventStart(buf)
	protocol.EventAddId(buf, eid)
	protocol.EventAddSender(buf, snd)
	protocol.EventAddEvent(buf, protocol.EventTypeFIND_NODE)
	protocol.EventAddResponse(buf, false)
	protocol.EventAddPayload(buf, fn)

	e := protocol.EventEnd(buf)

	buf.Finish(e)

	return buf.FinishedBytes()
}

func eventFindNodeResponse(buf *flatbuffers.Builder, id, sender []byte, nodes []*node) []byte {
	buf.Reset()

	// construct the node vector
	ns := make([]flatbuffers.UOffsetT, len(nodes))

	for i, n := range nodes {
		nid := buf.CreateByteVector(n.id)
		nad := buf.CreateByteVector([]byte(n.address.String()))

		protocol.NodeStart(buf)
		protocol.NodeAddId(buf, nid)
		protocol.NodeAddAddress(buf, nad)
		ns[i] = protocol.NodeEnd(buf)
	}

	protocol.FindNodeStartNodesVector(buf, len(nodes))

	// prepend nodes to vector in reverse order
	for i := len(nodes) - 1; i >= 0; i-- {
		buf.PrependUOffsetT(ns[i])
	}

	nv := buf.EndVector(len(nodes))

	// construct the find node table
	protocol.FindNodeStart(buf)
	protocol.FindNodeAddNodes(buf, nv)
	fn := protocol.FindNodeEnd(buf)

	// construct the response event table
	eid := buf.CreateByteVector(id)
	snd := buf.CreateByteVector(sender)

	protocol.EventStart(buf)
	protocol.EventAddId(buf, eid)
	protocol.EventAddSender(buf, snd)
	protocol.EventAddEvent(buf, protocol.EventTypeFIND_NODE)
	protocol.EventAddResponse(buf, true)
	protocol.EventAddPayload(buf, fn)

	e := protocol.EventEnd(buf)

	buf.Finish(e)

	return buf.FinishedBytes()
}

func eventFindValueRequest(buf *flatbuffers.Builder, id, sender, key []byte) []byte {
	buf.Reset()

	// create the find value table
	k := buf.CreateByteVector(key)

	protocol.FindValueStart(buf)
	protocol.FindValueAddKey(buf, k)
	fv := protocol.FindValueEnd(buf)

	// build the event to send
	eid := buf.CreateByteVector(id)
	snd := buf.CreateByteVector(sender)

	protocol.EventStart(buf)
	protocol.EventAddId(buf, eid)
	protocol.EventAddSender(buf, snd)
	protocol.EventAddEvent(buf, protocol.EventTypeFIND_VALUE)
	protocol.EventAddResponse(buf, false)
	protocol.EventAddPayload(buf, fv)

	e := protocol.EventEnd(buf)

	buf.Finish(e)

	return buf.FinishedBytes()
}

func eventFindValueFoundResponse(buf *flatbuffers.Builder, id, sender, value []byte) []byte {
	buf.Reset()

	v := buf.CreateByteVector(value)

	protocol.FindValueStart(buf)
	protocol.FindValueAddValue(buf, v)
	fv := protocol.FindValueEnd(buf)

	// construct the response event table
	eid := buf.CreateByteVector(id)
	snd := buf.CreateByteVector(sender)

	protocol.EventStart(buf)
	protocol.EventAddId(buf, eid)
	protocol.EventAddSender(buf, snd)
	protocol.EventAddEvent(buf, protocol.EventTypeFIND_VALUE)
	protocol.EventAddResponse(buf, true)
	protocol.EventAddPayload(buf, fv)

	e := protocol.EventEnd(buf)

	buf.Finish(e)

	return buf.FinishedBytes()
}

func eventFindValueNotFoundResponse(buf *flatbuffers.Builder, id, sender []byte, nodes []*node) []byte {
	buf.Reset()

	// construct the node vector
	ns := make([]flatbuffers.UOffsetT, len(nodes))

	for i, n := range nodes {
		nid := buf.CreateByteVector(n.id)
		nad := buf.CreateByteVector([]byte(n.address.String()))

		protocol.NodeStart(buf)
		protocol.NodeAddId(buf, nid)
		protocol.NodeAddAddress(buf, nad)
		ns[i] = protocol.NodeEnd(buf)
	}

	protocol.FindNodeStartNodesVector(buf, len(nodes))

	// prepend nodes to vector in reverse order
	for i := len(nodes) - 1; i >= 0; i-- {
		buf.PrependUOffsetT(ns[i])
	}

	nv := buf.EndVector(len(nodes))

	// construct the find node table
	protocol.FindValueStart(buf)
	protocol.FindValueAddNodes(buf, nv)
	fn := protocol.FindValueEnd(buf)

	// construct the response event table
	eid := buf.CreateByteVector(id)
	snd := buf.CreateByteVector(sender)

	protocol.EventStart(buf)
	protocol.EventAddId(buf, eid)
	protocol.EventAddSender(buf, snd)
	protocol.EventAddEvent(buf, protocol.EventTypeFIND_VALUE)
	protocol.EventAddResponse(buf, true)
	protocol.EventAddPayload(buf, fn)

	e := protocol.EventEnd(buf)

	buf.Finish(e)

	return buf.FinishedBytes()
}

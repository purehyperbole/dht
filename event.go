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

func eventFindValueRequest(buf *flatbuffers.Builder, id, sender, key []byte) []byte {
	buf.Reset()

	// create the find value table
	k := buf.CreateByteVector(key)

	protocol.FindValueStart(buf)
	protocol.FindValueAddKey(buf, k)
	fv := protocol.FindValueEnd(buf)

	// build the event to send
	eid := buf.CreateByteVector(rid)
	snd := buf.CreateByteVector(d.config.LocalID)

	protocol.EventStart(buf)
	protocol.EventAddId(buf, eid)
	protocol.EventAddSender(buf, snd)
	protocol.EventAddEvent(buf, protocol.EventTypeFIND_VALUE)
	protocol.EventAddResponse(buf, false)
	protocol.EventAddPayload(buf, fv)

	e := protocol.EventEnd(buf)

	buf.Finish(e)
}

package dht

import (
	"errors"
	"log"
	"net"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/purehyperbole/dht/protocol"
)

// a udp socket listener that processes incoming and outgoing packets
type listener struct {
	// udp listener
	conn *net.UDPConn
	// routing table
	routing *routingTable
	// request cache
	cache *cache
	// storage for all values
	storage *storage
	// the amount of time before a request expires and times out
	timeout time.Duration
	// flatbuffers buffer
	buffer *flatbuffers.Builder
	// local node id
	localID []byte
}

func newListener(conn *net.UDPConn, localID []byte, routing *routingTable, cache *cache, storage *storage, timeout time.Duration) *listener {
	l := &listener{
		conn:    conn,
		routing: routing,
		cache:   cache,
		storage: storage,
		timeout: timeout,
		buffer:  flatbuffers.NewBuilder(65527),
		localID: localID,
	}

	go l.process()

	return l
}

func (l *listener) process() {
	// buffer maximum udp payload
	b := make([]byte, 65527)

	for {
		rb, addr, err := l.conn.ReadFromUDP(b)
		if err != nil {
			panic(err)
		}

		log.Println("received event from:", addr, "size:", rb)

		e := protocol.GetRootAsEvent(b[:rb], 0)

		// update the senders last seen time in the routing table
		l.routing.seen(e.SenderBytes())

		// if this is a response to a query, send the response event to
		// the registered callback
		if e.Response() {
			callback, ok := l.cache.pop(e.IdBytes())
			if ok {
				callback(e, nil)
			}

			continue
		}

		// handle request
		switch e.Event() {
		case protocol.EventTypePING:
			err = l.pong(e, addr)
		case protocol.EventTypeSTORE:
			err = l.store(e, addr)
		case protocol.EventTypeFIND_NODE:
			err = l.findNode(e, addr)
		case protocol.EventTypeFIND_VALUE:
			err = l.findValue(e, addr)
		}

		if err != nil {
			log.Println("failed to handle request: ", err.Error())
		}
	}
}

// send a pong response to the sender
func (l *listener) pong(event *protocol.Event, addr *net.UDPAddr) error {
	resp := eventPong(l.buffer, event.IdBytes(), l.localID)

	_, err := l.conn.WriteToUDP(resp, addr)

	return err
}

// store a value from the sender and send a response to confirm
func (l *listener) store(event *protocol.Event, addr *net.UDPAddr) error {
	payloadTable := new(flatbuffers.Table)

	if !event.Payload(payloadTable) {
		return errors.New("invalid store request payload")
	}

	s := new(protocol.Store)
	s.Init(payloadTable.Bytes, payloadTable.Pos)

	l.storage.set(s.KeyBytes(), s.ValueBytes(), time.Unix(s.Ttl(), 0))

	resp := eventStoreResponse(l.buffer, event.IdBytes(), l.localID, s.KeyBytes())

	_, err := l.conn.WriteToUDP(resp, addr)

	return err
}

// find all given nodes
func (l *listener) findNode(event *protocol.Event, addr *net.UDPAddr) error {
	payloadTable := new(flatbuffers.Table)

	if !event.Payload(payloadTable) {
		return errors.New("invalid find node request payload")
	}

	f := new(protocol.FindNode)
	f.Init(payloadTable.Bytes, payloadTable.Pos)

	// find the K closest neighbours to the given target
	nodes := l.routing.closestN(f.KeyBytes(), K)

	resp := eventFindNodeResponse(l.buffer, event.IdBytes(), l.localID, nodes)

	_, err := l.conn.WriteToUDP(resp, addr)

	return err
}

func (l *listener) findValue(event *protocol.Event, addr *net.UDPAddr) error {
	payloadTable := new(flatbuffers.Table)

	if !event.Payload(payloadTable) {
		return errors.New("invalid find node request payload")
	}

	f := new(protocol.FindValue)
	f.Init(payloadTable.Bytes, payloadTable.Pos)

	v, ok := l.storage.get(f.KeyBytes())

	var resp []byte

	// TODO : clean this up
	if ok {
		// we found the key in our storage, so we return it to the requester
		// construct the find node table
		resp = eventFindValueFoundResponse(l.buffer, event.IdBytes(), l.localID, v)
	} else {
		// we didn't find the key, so we find the K closest neighbours to the given target
		nodes := l.routing.closestN(f.KeyBytes(), K)
		resp = eventFindValueNotFoundResponse(l.buffer, event.IdBytes(), l.localID, nodes)
	}

	_, err := l.conn.WriteToUDP(resp, addr)

	return err
}

func (l *listener) request(to *net.UDPAddr, id []byte, data []byte, cb func(event *protocol.Event, err error)) error {
	// register the callback for this request
	l.cache.set(id, time.Now().Add(l.timeout), cb)

	_, err := l.conn.WriteToUDP(data, to)

	return err
}

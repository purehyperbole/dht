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
	// storage for all values
	storage Storage
	// udp listener
	conn *net.UDPConn
	// routing table
	routing *routingTable
	// request cache
	cache *cache
	// the amount of time before a request expires and times out
	timeout time.Duration
	// flatbuffers buffer
	buffer *flatbuffers.Builder
	// local node id
	localID []byte
}

func newListener(conn *net.UDPConn, localID []byte, routing *routingTable, cache *cache, storage Storage, timeout time.Duration) *listener {
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
			if errors.Is(err, net.ErrClosed) {
				// network connection closed, so
				// we can shutdown
				return
			}
			panic(err)
		}

		// log.Println("received event from", addr.String())

		var transferKeys bool

		// log.Println("received event from:", addr, "size:", rb)

		e := protocol.GetRootAsEvent(b[:rb], 0)

		// attempt to update the node first, but if it doesn't exist, insert it
		if !l.routing.seen(e.SenderBytes()) {
			// insert/update the node in the routing table
			nid := make([]byte, e.SenderLength())
			copy(nid, e.SenderBytes())

			l.routing.insert(nid, addr)

			// this node is new to us, so we should send it any
			// keys that are closer to it than to us
			transferKeys = true
		}

		// if this is a response to a query, send the response event to
		// the registered callback
		if e.Response() {
			// update the senders last seen time in the routing table
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
			continue
		}

		// TODO : this is going to end up with the receiver being ddos'ed
		// with keys if storage is holding a large amount of values
		// also, it's going to receive duplicate keys from other nodes?
		// this will also lock our storage map and make us unresponsive to
		// requests, potentially taking us out of other nodes routing tables.
		// that may have a cascading effect...
		if transferKeys {
			l.storage.Iterate(func(key, value []byte, ttl time.Duration) bool {
				// TODO : keeping storage locked while we do socket io is not ideal
				d1 := distance(l.localID, key)
				d2 := distance(e.SenderBytes(), key)

				if d2 > d1 {
					// other node has more matching bits to the key, so we send it the value
					rid := randomID()
					req := eventStoreRequest(l.buffer, rid, l.localID, key, value, int64(ttl))

					err = l.request(addr, rid, req, func(ev *protocol.Event, err error) {
						if err != nil {
							// just log this error for now, but it might be best to attempt to resend?
							log.Println(err)
						}
					})

					if err != nil {
						// log error and stop sending
						log.Println(err)
						return false
					}
				}

				return true
			})

			// fmt.Printf("transferred %d keys from %s to %s in %s\n", sent, l.conn.LocalAddr(), addr.String(), time.Since(start).String())
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

	l.storage.Set(s.KeyBytes(), s.ValueBytes(), time.Duration(s.Ttl()))

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

	v, ok := l.storage.Get(f.KeyBytes())

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

	// log.Println("sending event to", to.String())

	_, err := l.conn.WriteToUDP(data, to)

	return err
}

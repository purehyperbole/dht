package dht

import (
	"errors"
	"log"
	"net"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/purehyperbole/dht/protocol"
)

const (
	// MaxPayloadSize the maximum udp packet size
	// setting this above MTU will cause fragmentation
	// on networks that don't support it
	MaxPayloadSize = 65024
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

	// TODO : implement a packet reassembler
	// setup the size of our read and write buffers
	// this will not be good on networks that have a
	// smaller frame size than 64kb
	err := l.conn.SetReadBuffer(1<<16 - 1)
	if err != nil {
		log.Fatal(err)
	}

	err = l.conn.SetWriteBuffer(1<<16 - 1)
	if err != nil {
		log.Fatal(err)
	}

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
			l.transferKeys(addr, e.SenderBytes())
		}
	}
}

// send a pong response to the sender
func (l *listener) pong(event *protocol.Event, addr *net.UDPAddr) error {
	resp := eventPong(l.buffer, event.IdBytes(), l.localID)

	return l.write(addr, resp)
}

// store a value from the sender and send a response to confirm
func (l *listener) store(event *protocol.Event, addr *net.UDPAddr) error {
	payloadTable := new(flatbuffers.Table)

	if !event.Payload(payloadTable) {
		return errors.New("invalid store request payload")
	}

	s := new(protocol.Store)
	s.Init(payloadTable.Bytes, payloadTable.Pos)

	/*
		if s.ValuesLength() > 1 {
			log.Println("batch receiving", s.ValuesLength(), "from", addr.String(), "to", l.conn.LocalAddr())
		}
	*/

	for i := 0; i < s.ValuesLength(); i++ {
		v := new(protocol.Value)
		if s.Values(v, i) {
			l.storage.Set(v.KeyBytes(), v.ValueBytes(), time.Duration(v.Ttl()))
		}
	}

	resp := eventStoreResponse(l.buffer, event.IdBytes(), l.localID)

	return l.write(addr, resp)
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

	return l.write(addr, resp)
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
		resp = eventFindValueFoundResponse(l.buffer, event.IdBytes(), l.localID, v.Value)
	} else {
		// we didn't find the key, so we find the K closest neighbours to the given target
		nodes := l.routing.closestN(f.KeyBytes(), K)
		resp = eventFindValueNotFoundResponse(l.buffer, event.IdBytes(), l.localID, nodes)
	}

	return l.write(addr, resp)
}

func (l *listener) transferKeys(to *net.UDPAddr, id []byte) {
	l.buffer.Reset()

	// we can fix a maximum of ~1055 values into a single udp packet, assuming empty values.
	// calculated as: 65535 - 112 (event overhead) / 62 (value table with value length of 0)
	values := make([]*Value, 0, 1100)
	var size int // total size of the current values

	l.storage.Iterate(func(value *Value) bool {
		d1 := distance(l.localID, value.Key)
		d2 := distance(id, value.Key)

		if d2 > d1 {
			// if we cant fit any more values in this event, send it
			if size >= MaxPayloadSize {
				rid := randomID()
				req := eventStoreRequest(l.buffer, rid, l.localID, values)

				// log.Println("batch sending", len(values), "from", l.conn.LocalAddr().String(), "to", to.String(), "in", time.Since(start))

				err := l.request(to, rid, req, func(ev *protocol.Event, err error) {
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

				// reset the values array and size
				values = values[:0]
				size = 0
			}

			// add the remaining value to the array
			// for the next packet. 42 is the overhead
			// of the data in the value table
			values = append(values, value)
			size = size + len(value.Key) + len(value.Value) + 42

			return true
		}

		return true
	})

	// send any unfinished values
	if len(values) > 0 {
		rid := randomID()
		req := eventStoreRequest(l.buffer, rid, l.localID, values)

		// log.Println("batch sending", len(values), "from", l.conn.LocalAddr().String(), "to", to.String(), "in", time.Since(start))

		err := l.request(to, rid, req, func(ev *protocol.Event, err error) {
			if err != nil {
				// just log this error for now, but it might be best to attempt to resend?
				log.Println(err)
			}
		})

		if err != nil {
			// log error and stop sending
			log.Println(err)
		}
	}
}

func (l *listener) request(to *net.UDPAddr, id []byte, data []byte, cb func(event *protocol.Event, err error)) error {
	// register the callback for this request
	l.cache.set(id, time.Now().Add(l.timeout), cb)

	// log.Println("sending event to", to.String())

	return l.write(to, data)
}

func (l *listener) write(to *net.UDPAddr, data []byte) error {
	/*
		if len(data) > 1350 {
			// we may be exceeding MTU here and getting fragmented
			log.Println("send possibly fragmented size:", len(data))
		}
	*/

	_, err := l.conn.WriteToUDP(data, to)

	return err
}

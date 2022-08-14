package dht

import (
	"encoding/hex"
	"errors"
	"log"
	"net"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/purehyperbole/dht/protocol"
	"golang.org/x/net/ipv4"
)

// a udp socket listener that processes incoming and outgoing packets
type listener struct {
	// udp listener
	conn *ipv4.PacketConn
	// routing table
	routing *routingTable
	// request cache
	cache *cache
	// storage for all values
	storage Storage
	// packet manager for large packets
	packet *packetManager
	// flatbuffers buffer
	buffer *flatbuffers.Builder
	// local node id
	localID []byte
	// the amount of time before a request expires and times out
	timeout time.Duration
	// the size in bytes of the sockets send and receive buffer
	bufferSize int
	// collection of messages that will be read to in batch from the underlying socket
	readBatch []ipv4.Message
	// collection of messages that will be written in batch to the underlying socket
	writeBatch []ipv4.Message
	// size of the current write batch
	writeBatchSize int
	// mutex to protect writes to the write batch
	mu sync.Mutex
	// timer to schedule flushes to the underlying socket
	ftimer *time.Ticker
	// enables basic logging
	logging bool
}

func (l *listener) process() {
	for {
		bs, err := l.conn.ReadBatch(l.readBatch, 0)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				// network connection closed, so
				// we can shutdown
				return
			}
			panic(err)
		}

		for i := 0; i < bs; i++ {
			// if we have a fragmented packet, continue reading data
			p := l.packet.assemble(l.readBatch[i].Buffers[0][:l.readBatch[i].N])
			if p == nil {
				continue
			}

			addr := l.readBatch[i].Addr.(*net.UDPAddr)

			var transferKeys bool

			// log.Println("received event from:", addr, "size:", rb)

			e := protocol.GetRootAsEvent(p.data(), 0)

			// attempt to update the node first, but if it doesn't exist, insert it
			if !l.routing.seen(e.SenderBytes()) {
				if l.logging {
					log.Printf("discovered new node id: %s address: %s", hex.EncodeToString(e.SenderBytes()), addr.String())
				}

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

				l.packet.done(p)

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
				l.packet.done(p)
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

			l.packet.done(p)
		}
	}

}

// send a pong response to the sender
func (l *listener) pong(event *protocol.Event, addr *net.UDPAddr) error {
	resp := eventPong(l.buffer, event.IdBytes(), l.localID)

	return l.write(addr, event.IdBytes(), resp)
}

// store a value from the sender and send a response to confirm
func (l *listener) store(event *protocol.Event, addr *net.UDPAddr) error {
	payloadTable := new(flatbuffers.Table)

	if !event.Payload(payloadTable) {
		return errors.New("invalid store request payload")
	}

	s := new(protocol.Store)
	s.Init(payloadTable.Bytes, payloadTable.Pos)

	for i := 0; i < s.ValuesLength(); i++ {
		v := new(protocol.Value)
		if s.Values(v, i) {
			l.storage.Set(v.KeyBytes(), v.ValueBytes(), time.Duration(v.Ttl()))
		}
	}

	resp := eventStoreResponse(l.buffer, event.IdBytes(), l.localID)

	return l.write(addr, event.IdBytes(), resp)
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

	return l.write(addr, event.IdBytes(), resp)
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

	return l.write(addr, event.IdBytes(), resp)
}

func (l *listener) transferKeys(to *net.UDPAddr, id []byte) {
	l.buffer.Reset()

	// we can fix a maximum of ~1055 values into a single udp packet, assuming empty values.
	// calculated as: 65535 - 112 (event overhead) / 62 (value table with value length of 0)
	values := make([]*Value, 0, 1100)
	var size int // total size of the current values

	// determine whether we should transfer all nodes if the number of nodes in the network is
	// below the replication factor
	transferAll := l.routing.neighbours() < K

	l.storage.Iterate(func(value *Value) bool {
		d1 := distance(l.localID, value.Key)
		d2 := distance(id, value.Key)

		if transferAll || d2 > d1 {
			// if we cant fit any more values in this event, send it
			if size >= MaxEventSize {
				rid := pseudorandomID()
				req := eventStoreRequest(l.buffer, rid, l.localID, values)

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
		rid := pseudorandomID()
		req := eventStoreRequest(l.buffer, rid, l.localID, values)

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

	return l.write(to, id, data)
}

func (l *listener) write(to *net.UDPAddr, id, data []byte) error {
	p := l.packet.fragment(id, data)
	defer l.packet.done(p)

	f := p.next()

	l.mu.Lock()
	defer l.mu.Unlock()

	for f != nil {
		l.writeBatch[l.writeBatchSize].Addr = to
		// set the len of the buffer without allocating a new buffer
		l.writeBatch[l.writeBatchSize].Buffers[0] = l.writeBatch[l.writeBatchSize].Buffers[0][:len(f)]
		// copy the data from the fragment buffer into the message buffer
		copy(l.writeBatch[l.writeBatchSize].Buffers[0], f)

		l.writeBatchSize++

		if l.writeBatchSize >= len(l.writeBatch) {
			err := l.flush(false)
			if err != nil {
				return err
			}
		}

		f = p.next()
	}

	return nil
}

func (l *listener) flusher() {
	for {
		_, ok := <-l.ftimer.C
		if !ok {
			return
		}

		err := l.flush(true)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				// network connection closed, so
				// we can shutdown
				return
			}
			panic(err)
		}
	}
}

func (l *listener) flush(lock bool) error {
	if lock {
		l.mu.Lock()
		defer l.mu.Unlock()
	}

	if l.writeBatchSize < 1 {
		return nil
	}

	_, err := l.conn.WriteBatch(l.writeBatch[:l.writeBatchSize], 0)
	if err != nil {
		return err
	}

	// reset the batch
	l.writeBatchSize = 0

	return nil
}
